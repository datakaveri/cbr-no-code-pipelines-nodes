"""
Patient Measurement Clustering — CBR UDF
-----------------------------------------
Input  : OMOP measurement table (Parquet) from upstream node via NODE_CONTEXT
Output : result.parquet with PCA coords + cluster labels + clinical scores

Runtime contract:
  - Reads NODE_CONTEXT (JSON env var) for inputs/outputs/config
  - All storage reads/writes go through storage_v2 (presigned URLs via the
    backend's API-key-authenticated API); DuckDB itself only ever touches
    the local temp file storage_v2 downloads to, never S3 directly.
  - Writes output to the exact path in output.files[0].path
  - Prints a JSON status line to stdout; exits 0 on success / 1 on failure
"""

import json
import os
import sys
import tempfile

import duckdb
import requests
from storage_v2 import open_write, read_to_file
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans, AgglomerativeClustering, SpectralClustering
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.decomposition import PCA


NODE_PREFIX = "[PATIENT CLUSTERING]"


def log(msg):
    print(f"{NODE_PREFIX} {msg}", flush=True)


def log_error(msg):
    print(f"{NODE_PREFIX} ERROR: {msg}", file=sys.stderr, flush=True)


def _build_clusterer(algorithm: str, n_clusters: int):
    if algorithm == "agglomerative":
        return AgglomerativeClustering(n_clusters=n_clusters, linkage="ward")
    if algorithm == "spectral":
        return SpectralClustering(n_clusters=n_clusters, random_state=42, affinity="nearest_neighbors")
    return KMeans(n_clusters=n_clusters, random_state=42, n_init=10)


def _range_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-patient deviation scores using OMOP range_low / range_high.
    Works for any measurement schema — no hardcoded test names.

    Returns a DataFrame with one row per person_id:
      abnormality_score  – sum of fractional excess above range_high (×100)
      deficiency_score   – sum of fractional deficit below range_low  (×100)
      out_of_range_count – number of measurements outside reference range
    """
    cols = ["person_id", "value_as_number", "range_low", "range_high"]
    d = df[cols].copy()
    has_range = d["range_high"].notna() & d["range_low"].notna()

    if not has_range.any():
        pids = df["person_id"].unique()
        return pd.DataFrame({
            "person_id": pids,
            "abnormality_score": 0.0,
            "deficiency_score": 0.0,
            "out_of_range_count": 0,
        })

    d = d[has_range].copy()
    safe_high = d["range_high"].replace(0, np.nan)
    safe_low  = d["range_low"].replace(0, np.nan)

    d["above"] = ((d["value_as_number"] - d["range_high"]) / safe_high).clip(lower=0).fillna(0)
    d["below"] = ((d["range_low"] - d["value_as_number"]) / safe_low).clip(lower=0).fillna(0)
    d["oor"]   = (d["above"] > 0) | (d["below"] > 0)

    return (
        d.groupby("person_id")
         .agg(
             abnormality_score =("above", lambda x: round(x.sum() * 100, 2)),
             deficiency_score  =("below", lambda x: round(x.sum() * 100, 2)),
             out_of_range_count=("oor",   "sum"),
         )
         .reset_index()
    )


def cluster_patients(df: pd.DataFrame, n_clusters: int, algorithm: str = "kmeans") -> pd.DataFrame:
    pivot = df.pivot_table(
        index="person_id",
        columns="measurement_source_value",
        values="value_as_number",
        aggfunc="mean",
    ).reset_index()

    person_ids = pivot["person_id"]
    feature_cols = [c for c in pivot.columns if c != "person_id"]
    X = pivot[feature_cols]

    X_imputed = SimpleImputer(strategy="mean").fit_transform(X)
    X_scaled  = StandardScaler().fit_transform(X_imputed)
    labels    = _build_clusterer(algorithm, n_clusters).fit_predict(X_scaled)

    pca   = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)
    log(f"PCA variance explained: PC1={pca.explained_variance_ratio_[0]*100:.1f}%  PC2={pca.explained_variance_ratio_[1]*100:.1f}%")

    rs = _range_scores(df).set_index("person_id")
    has_range_data = (rs["out_of_range_count"] > 0).any()

    if has_range_data:
        composite_risk = np.array([
            rs.loc[pid, "out_of_range_count"] + rs.loc[pid, "abnormality_score"] / 100
            if pid in rs.index else 0.0
            for pid in person_ids.values
        ])
    else:
        composite_risk = X_scaled.mean(axis=1)

    cluster_means = {c: composite_risk[labels == c].mean() for c in range(n_clusters)}
    sorted_clusters = sorted(cluster_means, key=cluster_means.get)
    risk_names = ["Low Risk", "Moderate Risk", "High Risk"]
    name_map = {
        c: risk_names[i] if n_clusters == 3 else f"Cluster {i}"
        for i, c in enumerate(sorted_clusters)
    }

    abnormality_score  = rs.reindex(person_ids.values)["abnormality_score"].fillna(0).values
    deficiency_score   = rs.reindex(person_ids.values)["deficiency_score"].fillna(0).values
    out_of_range_count = rs.reindex(person_ids.values)["out_of_range_count"].fillna(0).astype(int).values

    return pd.DataFrame({
        "person_id":          person_ids.values,
        "pca_x":              X_pca[:, 0].round(4),
        "pca_y":              X_pca[:, 1].round(4),
        "cluster":            labels,
        "cluster_label":      [name_map[l] for l in labels],
        "abnormality_score":  abnormality_score,
        "deficiency_score":   deficiency_score,
        "out_of_range_count": out_of_range_count,
        "num_measurements":   df.groupby("person_id")["measurement_id"].count()
                                .reindex(person_ids.values).values,
    })


def main():
    ctx = json.loads(os.environ["NODE_CONTEXT"])
    node_name = ctx["node"]["name"]
    config = ctx.get("config", {})
    inputs = ctx.get("inputs", [])
    out_files = ctx["output"]["files"]

    n_clusters = int(config.get("n_clusters", 3))
    algorithm  = config.get("algorithm", "kmeans")
    local_input = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    local_input.close()
    read_to_file(inputs[0]["output"]["path"], local_input.name)
    input_path = local_input.name
    output_file = out_files[0]
    output_path = output_file["path"]
    presigned_put_url = output_file.get("presignedUrl")

    log(f"=== {node_name} ===")
    log(f"Input    : {input_path}")
    log(f"Output   : {output_path}")
    log(f"Algorithm: {algorithm}, n_clusters: {n_clusters}")

    conn = duckdb.connect(":memory:")

    log("Reading input parquet...")
    df = conn.execute(f"SELECT * FROM read_parquet('{input_path}')").df()
    log(f"Loaded {len(df)} rows, {df['person_id'].nunique()} patients")

    required = {"person_id", "measurement_source_value", "value_as_number"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required OMOP columns: {missing}")

    log(f"Running {algorithm} clustering...")
    result = cluster_patients(df, n_clusters, algorithm)
    log(f"Output: {len(result)} patients across {n_clusters} clusters")

    conn.register("result_table", result)

    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        conn.execute(f"COPY result_table TO '{tmp.name}' (FORMAT PARQUET, COMPRESSION 'snappy')")
        with open(tmp.name, "rb") as source, open_write(output_path) as destination:
            while block := source.read(1024 * 1024):
                destination.write(block)
    log("Done.")
    print(json.dumps({
        "success": True,
        "nodeName": node_name,
        "outputs": [{"name": f["name"], "path": f["path"]} for f in out_files],
    }))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log_error(str(exc))
        import traceback; traceback.print_exc()
        print(json.dumps({"success": False, "error": str(exc)}))
        sys.exit(1)
