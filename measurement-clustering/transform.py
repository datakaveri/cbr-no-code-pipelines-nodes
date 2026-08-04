#!/usr/bin/env python3
"""
Patient Measurement Clustering - Custom Node Container

Pivots an OMOP-format measurement table (person_id, measurement_id,
measurement_source_value, value_as_number) so each patient becomes one row,
runs KMeans clustering on the resulting feature matrix, and writes a
per-patient summary table with PCA coordinates, a human-readable severity
cluster label, and clinical summary scores (HAM-D, PSQI, CDR). Uses DuckDB
for S3 I/O, following the same pattern as the transformer node.

Reads two S3 credential sets (same contract as the transformer node):
  INPUT_S3_*     - upload bucket (user source files), read-only
  ARTIFACT_S3_*  - artifact bucket (workflow outputs), read+write
Writes output to the exact path the platform assigns in output.files[0].path.
"""

import os
import sys
import json
import tempfile
import duckdb
import requests
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.decomposition import PCA


def log(msg):
    print(f"[CLUSTERING] {msg}", flush=True)


def log_error(msg):
    print(f"[CLUSTERING ERROR] {msg}", file=sys.stderr, flush=True)


def create_s3_secret(conn, name: str, prefix: str, bucket_env_var: str):
    """Create a DuckDB S3 secret scoped to one bucket from {prefix}_S3_* env vars.

    Fallback path only, used for an input/output that has no presignedUrl
    (see main()). Two secrets (INPUT + ARTIFACT) are created when their
    credential env vars are present; DuckDB picks the matching one per query
    from the path's bucket, so reads from the read-only upload bucket and
    writes to the read+write artifact bucket each use the right credentials.
    Skipped (not an error) when the env vars are absent — once a future
    backend stops injecting STS credentials, every entry is expected to carry
    a presignedUrl and this fallback is simply unused.
    """
    if not os.environ.get(f"{prefix}_S3_ACCESS_KEY"):
        log(f"No {prefix}_S3_ACCESS_KEY configured, skipping {name} (presigned-URL-only mode)")
        return

    bucket = os.environ[bucket_env_var]
    session_token = os.environ.get(f"{prefix}_S3_SESSION_TOKEN", "")
    token_clause = f",\n        SESSION_TOKEN '{session_token}'" if session_token else ""
    conn.execute(f"""
        CREATE SECRET {name} (
            TYPE S3,
            KEY_ID '{os.environ[f"{prefix}_S3_ACCESS_KEY"]}',
            SECRET '{os.environ[f"{prefix}_S3_SECRET_KEY"]}',
            ENDPOINT '{os.environ[f"{prefix}_S3_ENDPOINT"]}',
            SCOPE 's3://{bucket}',
            URL_STYLE 'path',
            USE_SSL {os.environ.get(f"{prefix}_S3_USE_SSL", "false").lower()},
            REGION '{os.environ.get(f"{prefix}_S3_REGION", "us-east-1")}'{token_clause}
        )
    """)


def setup_duckdb_s3(conn):
    conn.load_extension("httpfs")
    create_s3_secret(conn, "input_secret", "INPUT", "INPUT_S3_BUCKET")
    create_s3_secret(conn, "artifact_secret", "ARTIFACT", "ARTIFACT_S3_BUCKET")


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
    # A presigned GET URL needs no S3 secret at all — httpfs treats a plain
    # https:// argument as a generic HTTP resource. Falls back to the s3://
    # path (read via the secrets created above) when the backend hasn't
    # attached one.
    input_path = inputs[0]["output"].get("presignedUrl") or inputs[0]["output"]["path"]
    input_format = inputs[0]["output"].get("format", "parquet").lower()
    output_file = output["files"][0]
    output_path = output_file["path"]
    presigned_put_url = output_file.get("presignedUrl")

    log(f"=== Patient Clustering: {node['name']} ===")
    log(f"Input  : {input_path} (format: {input_format})")
    log(f"Output : {output_path}")
    log(f"n_clusters: {n_clusters}")

    conn = duckdb.connect(":memory:")
    setup_duckdb_s3(conn)

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

    if presigned_put_url:
        # Write to a local temp file and PUT it ourselves rather than
        # `COPY ... TO 'https://...'` — DuckDB's HTTP write support varies by
        # version, whereas a local COPY + `requests.put` of a presigned S3
        # URL works the same everywhere.
        log("Writing output parquet via presigned URL...")
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            conn.execute(f"COPY result_table TO '{tmp.name}' (FORMAT PARQUET, COMPRESSION 'snappy')")
            with open(tmp.name, "rb") as fh:
                resp = requests.put(presigned_put_url, data=fh.read(), timeout=600)
                resp.raise_for_status()
        log(f"  Exported to {output_path} via presigned URL")
    else:
        log("Writing output parquet to S3...")
        conn.execute(f"""
            COPY result_table TO '{output_path}'
            (FORMAT PARQUET, COMPRESSION 'snappy')
        """)

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
