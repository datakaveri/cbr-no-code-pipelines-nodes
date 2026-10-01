#!/usr/bin/env python3
"""
Patient Measurement Clustering - Custom Node Container

Pivots an OMOP-format measurement table (person_id, measurement_id,
measurement_source_value, value_as_number) so each patient becomes one row,
runs KMeans clustering on the resulting feature matrix, and writes a
per-patient summary table with PCA coordinates, a human-readable severity
cluster label, and clinical summary scores (HAM-D, PSQI, CDR). All storage
reads/writes go through storage_v2 (presigned URLs via the backend's
API-key-authenticated API); DuckDB itself only ever touches the local temp
file storage_v2 downloads to, never S3 directly.
Writes output to the exact path the platform assigns in output.files[0].path.
"""

import os
import sys
import json
import tempfile
import duckdb
import requests
from storage_v2 import open_write, read_to_file
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.decomposition import PCA


def log(msg):
    print(f"[CLUSTERING] {msg}", flush=True)


def log_error(msg):
    print(f"[CLUSTERING ERROR] {msg}", file=sys.stderr, flush=True)


def run_clustering(df: pd.DataFrame, n_clusters: int) -> pd.DataFrame:
    """Pivot OMOP measurement table -> KMeans + PCA -> per-patient summary table.

    Returns one row per patient with PCA coordinates, a numeric cluster id, a
    human-readable severity cluster label, clinical summary scores (HAM-D,
    PSQI, CDR), and the number of measurements recorded for that patient.
    """
    pivot = df.pivot_table(
        index="person_id",
        columns="measurement_source_value",
        values="value_as_number",
        aggfunc="mean"
    ).reset_index()

    person_ids = pivot["person_id"]
    X = pivot.drop(columns=["person_id"])

    X_imputed = SimpleImputer(strategy="mean").fit_transform(X)
    X_scaled  = StandardScaler().fit_transform(X_imputed)
    labels    = KMeans(n_clusters=n_clusters, random_state=42, n_init=10).fit_predict(X_scaled)

    for i in range(n_clusters):
        log(f"  Cluster {i}: {(labels==i).sum()} patients")

    pca   = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)
    log(f"PCA variance explained: PC1={pca.explained_variance_ratio_[0]*100:.1f}%  PC2={pca.explained_variance_ratio_[1]*100:.1f}%")

    # Clinical summary scores per patient
    ham_d_cols = [c for c in pivot.columns if "ham_d" in str(c)]
    psqi_cols  = [c for c in pivot.columns if "psqi" in str(c) and "cumulative" not in str(c)]
    cdr_cols   = [c for c in pivot.columns if "cdr" in str(c)]

    ham_d_total = pivot[ham_d_cols].sum(axis=1).round(2).values
    psqi_total  = pivot[psqi_cols].sum(axis=1).round(2).values
    cdr_score   = pivot[cdr_cols].mean(axis=1).round(2).values

    # Assign human-readable cluster names based on HAM-D severity
    cluster_means = {c: ham_d_total[labels == c].mean() for c in range(n_clusters)}
    sorted_clusters = sorted(cluster_means, key=cluster_means.get)
    severity_names = ["Low Severity", "Moderate Severity", "High Severity"]
    name_map = {
        c: severity_names[i] if n_clusters == 3 else f"Cluster {i}"
        for i, c in enumerate(sorted_clusters)
    }

    return pd.DataFrame({
        "person_id":        person_ids.values,
        "pca_x":            X_pca[:, 0].round(4),
        "pca_y":            X_pca[:, 1].round(4),
        "cluster":          labels,
        "cluster_label":    [name_map[l] for l in labels],
        "ham_d_total":      ham_d_total,
        "psqi_total":       psqi_total,
        "cdr_score":        cdr_score,
        "num_measurements": df.groupby("person_id")["measurement_id"].count().reindex(person_ids.values).values,
    })


def main():
    ctx        = json.loads(os.environ["NODE_CONTEXT"])
    node       = ctx["node"]
    inputs     = ctx["inputs"]
    output     = ctx["output"]
    config     = ctx["config"]

    n_clusters = int(config.get("n_clusters", 3))
    local_input = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    local_input.close()
    read_to_file(inputs[0]["output"]["path"], local_input.name)
    input_path = local_input.name
    input_format = inputs[0]["output"].get("format", "parquet").lower()
    output_file = output["files"][0]
    output_path = output_file["path"]
    presigned_put_url = output_file.get("presignedUrl")

    log(f"=== Patient Clustering: {node['name']} ===")
    log(f"Input  : {input_path} (format: {input_format})")
    log(f"Output : {output_path}")
    log(f"n_clusters: {n_clusters}")

    conn = duckdb.connect(":memory:")

    if input_format == "csv":
        read_func = "read_csv_auto"
    elif input_format == "json":
        read_func = "read_json_auto"
    else:
        read_func = "read_parquet"

    log("Reading input...")
    df = conn.execute(f"SELECT * FROM {read_func}('{input_path}')").df()
    log(f"Loaded {len(df)} rows, columns: {list(df.columns)}")

    required = {"person_id", "measurement_id", "measurement_source_value", "value_as_number"}
    missing  = required - set(df.columns)
    if missing:
        log_error(f"Missing required columns: {missing}")
        sys.exit(1)

    log("Running KMeans clustering...")
    result_df = run_clustering(df, n_clusters)
    log(f"Output has {len(result_df)} patients")

    conn.register("result_table", result_df)

    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        conn.execute(f"COPY result_table TO '{tmp.name}' (FORMAT PARQUET, COMPRESSION 'snappy')")
        with open(tmp.name, "rb") as source, open_write(output_path) as destination:
            while block := source.read(1024 * 1024):
                destination.write(block)
    log("=== Done ===")
    print(json.dumps({
        "success":      True,
        "nodeName":     node["name"],
        "totalPatients": len(result_df),
        "nClusters":    n_clusters,
        "clusterCounts": {
            str(i): int((result_df["cluster"] == i).sum())
            for i in range(n_clusters)
        },
    }))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
