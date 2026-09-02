#!/usr/bin/env python3
"""
Outlier Remover Node
Removes rows containing outlier values using IQR, Z-Score, or Isolation Forest.

IQR:              flags rows where any checked column falls outside Q1-1.5*IQR .. Q3+1.5*IQR
Z-Score:          flags rows where any checked column is more than N std deviations from mean
Isolation Forest: sklearn ML model that scores each row — useful for catching multi-column outliers

Reads two S3 credential sets (same contract as the transformer node):
  INPUT_S3_*     - upload bucket (user source files), read-only
  ARTIFACT_S3_*  - artifact bucket (workflow outputs), read+write
Writes output to the exact path the platform assigns in output.files[0].path.
"""

import os
import sys
import json
from io import BytesIO

import numpy as np
import pandas as pd
import requests
from sklearn.ensemble import IsolationForest
import storage_boto3 as boto3
from storage_v2 import read as storage_read
from botocore.config import Config


def log(msg):
    print(f"[OUTLIER] {msg}", flush=True)


def log_error(msg):
    print(f"[OUTLIER ERROR] {msg}", file=sys.stderr, flush=True)


def make_s3_client(prefix: str):
    """Build a boto3 S3 client from {prefix}_S3_* env vars (INPUT or ARTIFACT)."""
    use_ssl = os.environ.get(f"{prefix}_S3_USE_SSL", "false").lower() == "true"
    scheme = "https" if use_ssl else "http"
    return boto3.client(
        "s3",
        endpoint_url=f"{scheme}://{os.environ[f'{prefix}_S3_ENDPOINT']}",
        aws_access_key_id=os.environ[f"{prefix}_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ[f"{prefix}_S3_SECRET_KEY"],
        aws_session_token=os.environ.get(f"{prefix}_S3_SESSION_TOKEN") or None,
        region_name=os.environ.get(f"{prefix}_S3_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def s3_split(s3_path):
    path = s3_path.replace("s3://", "")
    bucket, key = path.split("/", 1)
    return bucket, key


def read_parquet(s3_path):
    # Always re-resolve at read time via storage_v2 (POST /api/node/storage/
    # download-url) instead of trusting a presignedUrl handed in NODE_CONTEXT:
    # that field is signed at workflow-submit time, before an upstream node in
    # this same run has written anything, so for same-run inputs it points at
    # a key that doesn't exist yet and 404s. storage_read() fetches a fresh
    # URL against the file's CURRENT version, which is race-free.
    log(f"Reading {s3_path}")
    return pd.read_parquet(BytesIO(storage_read(s3_path)))


def write_parquet(s3, df, s3_path, presigned_url=None):
    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    if presigned_url:
        resp = requests.put(presigned_url, data=buf.getvalue(), timeout=300)
        resp.raise_for_status()
        return
    bucket, key = s3_split(s3_path)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def remove_outliers_iqr(df, cols, multiplier):
    mask = pd.Series([True] * len(df), index=df.index)
    for col in cols:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - multiplier * iqr
        upper = q3 + multiplier * iqr
        col_mask = df[col].between(lower, upper)
        log(f"  {col}: [{lower:.2f}, {upper:.2f}] — {(~col_mask).sum()} outlier rows")
        mask &= col_mask
    return df[mask]


def remove_outliers_zscore(df, cols, threshold):
    mask = pd.Series([True] * len(df), index=df.index)
    for col in cols:
        mean = df[col].mean()
        std = df[col].std()
        if std == 0:
            log(f"  {col}: std=0, skipping")
            continue
        z = (df[col] - mean) / std
        col_mask = z.abs() <= threshold
        log(f"  {col}: threshold={threshold} — {(~col_mask).sum()} outlier rows")
        mask &= col_mask
    return df[mask]


def remove_outliers_isolation_forest(df, cols, contamination):
    clf = IsolationForest(contamination=contamination, random_state=42)
    preds = clf.fit_predict(df[cols])   # -1 = outlier, 1 = inlier
    outlier_count = (preds == -1).sum()
    log(f"  Isolation Forest detected {outlier_count} outlier rows (contamination={contamination})")
    return df[preds == 1]


def main():
    ctx        = json.loads(os.environ["NODE_CONTEXT"])
    config     = ctx["config"]
    inputs     = ctx["inputs"]
    output     = ctx["output"]
    node       = ctx["node"]

    method     = config.get("method", "iqr")
    cols_raw   = config.get("columns", "").strip()
    threshold  = config.get("threshold", "").strip()

    log(f"=== Outlier Remover: {node['name']} ===")
    log(f"Method: {method}")

    if not inputs:
        log_error("No inputs provided")
        sys.exit(1)

    # Outputs always go to the artifact bucket.
    artifact_client = make_s3_client("ARTIFACT")

    input_entry = inputs[0]["output"]
    input_path = input_entry["path"]
    df = read_parquet(input_path)
    rows_before = len(df)
    log(f"Loaded {rows_before} rows, {len(df.columns)} columns")

    # Resolve columns
    if cols_raw:
        cols = [c.strip() for c in cols_raw.split(",") if c.strip()]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            log_error(f"Columns not found: {missing}")
            sys.exit(1)
        non_numeric = [c for c in cols if not pd.api.types.is_numeric_dtype(df[c])]
        if non_numeric:
            log_error(f"Non-numeric columns (outlier detection only works on numbers): {non_numeric}")
            sys.exit(1)
    else:
        cols = df.select_dtypes(include="number").columns.tolist()
        if not cols:
            log_error("No numeric columns found")
            sys.exit(1)
        log(f"Auto-selected numeric columns: {cols}")

    # Run selected method
    if method == "iqr":
        multiplier = float(threshold) if threshold else 1.5
        log(f"IQR multiplier: {multiplier}")
        df = remove_outliers_iqr(df, cols, multiplier)

    elif method == "zscore":
        z_threshold = float(threshold) if threshold else 3.0
        log(f"Z-score threshold: {z_threshold}")
        df = remove_outliers_zscore(df, cols, z_threshold)

    elif method == "isolation_forest":
        contamination = float(threshold) if threshold else 0.05
        if not (0.0 < contamination < 0.5):
            log_error("Isolation Forest contamination must be between 0.0 and 0.5")
            sys.exit(1)
        df = remove_outliers_isolation_forest(df, cols, contamination)

    else:
        log_error(f"Unknown method: {method}")
        sys.exit(1)

    rows_after   = len(df)
    rows_removed = rows_before - rows_after
    log(f"Removed {rows_removed} outlier rows ({rows_removed/rows_before*100:.1f}%)")
    log(f"Remaining: {rows_after} rows")

    out_entry = output["files"][0]
    out_path = out_entry["path"]
    write_parquet(artifact_client, df, out_path, out_entry.get("presignedUrl"))

    print(json.dumps({
        "success": True,
        "nodeName": node["name"],
        "method": method,
        "rowsBefore": rows_before,
        "rowsAfter": rows_after,
        "rowsRemoved": rows_removed,
    }))
    log("=== Done ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_error(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
