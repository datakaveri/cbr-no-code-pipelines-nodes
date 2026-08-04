#!/usr/bin/env python3
"""
Correlation Report Node
Computes pairwise correlation between numeric columns and outputs a long-format table.

Output columns: column_a, column_b, correlation, strength, direction
  - correlation: -1.0 to 1.0
  - strength:    "strong" / "moderate" / "weak"
  - direction:   "positive" / "negative" / "none"

Long format (one row per pair) is ideal for Superset heatmap visualization.

Reads two S3 credential sets (same contract as the transformer node):
  INPUT_S3_*     - upload bucket (user source files), read-only
  ARTIFACT_S3_*  - artifact bucket (workflow outputs), read+write
Writes output to the exact path the platform assigns in output.files[0].path.

Prefers the presigned GET/PUT URL the platform attaches to each input/output
entry in NODE_CONTEXT (`presignedUrl`) — plain HTTPS, no S3 credentials
needed. Falls back to boto3 + the INPUT_S3_*/ARTIFACT_S3_* credential env
vars when an entry has no presignedUrl (dual mode during the rollout).
"""

import os
import sys
import json
from io import BytesIO

import pandas as pd
import requests
import boto3
from botocore.config import Config


def log(msg):
    print(f"[CORRELATION] {msg}", flush=True)


def log_error(msg):
    print(f"[CORRELATION ERROR] {msg}", file=sys.stderr, flush=True)


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


def read_parquet(s3, s3_path, presigned_url=None):
    if presigned_url:
        log(f"Reading {s3_path} via presigned URL")
        resp = requests.get(presigned_url, timeout=300)
        resp.raise_for_status()
        return pd.read_parquet(BytesIO(resp.content))
    bucket, key = s3_split(s3_path)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def write_parquet(s3, df, s3_path, presigned_url=None):
    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    if presigned_url:
        log(f"Writing {len(df)} rows to {s3_path} via presigned URL")
        resp = requests.put(presigned_url, data=buf.getvalue(), timeout=300)
        resp.raise_for_status()
        return
    bucket, key = s3_split(s3_path)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def label_strength(val):
    abs_val = abs(val)
    if abs_val >= 0.7:
        return "strong"
    elif abs_val >= 0.4:
        return "moderate"
    else:
        return "weak"


def label_direction(val):
    if val > 0.05:
        return "positive"
    elif val < -0.05:
        return "negative"
    else:
        return "none"


def main():
    ctx       = json.loads(os.environ["NODE_CONTEXT"])
    config    = ctx["config"]
    inputs    = ctx["inputs"]
    output    = ctx["output"]
    node      = ctx["node"]

    method    = config.get("method", "pearson")
    cols_raw  = config.get("columns", "").strip()

    log(f"=== Correlation Report: {node['name']} ===")
    log(f"Method: {method}")

    if not inputs:
        log_error("No inputs provided")
        sys.exit(1)

    # Inputs may live in either bucket; outputs always go to the artifact
    # bucket. Pick the client per path by the bucket in its s3:// URI.
    input_client = make_s3_client("INPUT")
    artifact_client = make_s3_client("ARTIFACT")
    artifact_bucket = os.environ["ARTIFACT_S3_BUCKET"]

    def client_for(s3_path):
        bucket, _ = s3_split(s3_path)
        return artifact_client if bucket == artifact_bucket else input_client

    input_entry = inputs[0]["output"]
    input_path = input_entry["path"]
    df = read_parquet(client_for(input_path), input_path, input_entry.get("presignedUrl"))
    log(f"Loaded {len(df)} rows, {len(df.columns)} columns")

    # Resolve columns
    if cols_raw:
        cols = [c.strip() for c in cols_raw.split(",") if c.strip()]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            log_error(f"Columns not found: {missing}")
            sys.exit(1)
        non_numeric = [c for c in cols if not pd.api.types.is_numeric_dtype(df[c])]
        if non_numeric:
            log_error(f"Non-numeric columns: {non_numeric}. Correlation only works on numbers.")
            sys.exit(1)
    else:
        cols = df.select_dtypes(include="number").columns.tolist()
        if not cols:
            log_error("No numeric columns found")
            sys.exit(1)
        log(f"Auto-selected numeric columns: {cols}")

    if len(cols) < 2:
        log_error("Need at least 2 numeric columns to compute correlation")
        sys.exit(1)

    # Compute correlation matrix
    corr_matrix = df[cols].corr(method=method)
    log(f"Computed {method} correlation for {len(cols)} columns")

    # Convert wide matrix → long format (one row per unique pair, no duplicates)
    rows = []
    for i, col_a in enumerate(cols):
        for j, col_b in enumerate(cols):
            if i >= j:          # skip self-correlation and duplicate pairs
                continue
            val = round(corr_matrix.loc[col_a, col_b], 4)
            rows.append({
                "column_a":   col_a,
                "column_b":   col_b,
                "correlation": val,
                "strength":   label_strength(val),
                "direction":  label_direction(val),
            })

    result_df = pd.DataFrame(rows).sort_values("correlation", key=abs, ascending=False)
    log(f"Generated {len(result_df)} column pairs")

    # Log top 5 strongest correlations
    log("Top correlations:")
    for _, row in result_df.head(5).iterrows():
        log(f"  {row['column_a']} ↔ {row['column_b']}: {row['correlation']} ({row['strength']} {row['direction']})")

    out_entry = output["files"][0]
    out_path = out_entry["path"]
    write_parquet(artifact_client, result_df, out_path, out_entry.get("presignedUrl"))

    print(json.dumps({
        "success": True,
        "nodeName": node["name"],
        "method": method,
        "columnCount": len(cols),
        "pairCount": len(result_df),
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
