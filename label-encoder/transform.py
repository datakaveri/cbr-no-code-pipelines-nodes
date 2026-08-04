#!/usr/bin/env python3
"""
Label Encoder Node
Converts categorical/text columns into integer labels using scikit-learn's LabelEncoder.

Each unique value in a column gets a number: e.g. ['male','female'] -> [1, 0].
Adds new '<column>_encoded' columns. Optionally keeps or drops the originals.

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
from sklearn.preprocessing import LabelEncoder
import requests
import boto3
from botocore.config import Config


def log(msg):
    print(f"[ENCODER] {msg}", flush=True)


def log_error(msg):
    print(f"[ENCODER ERROR] {msg}", file=sys.stderr, flush=True)


def make_s3_client(prefix: str):
    """Build a boto3 S3 client from {prefix}_S3_* env vars (INPUT or ARTIFACT)."""
    use_ssl = os.environ.get(f"{prefix}_S3_USE_SSL", "false").lower() == "true"
    endpoint = os.environ[f"{prefix}_S3_ENDPOINT"]
    scheme = "https" if use_ssl else "http"

    return boto3.client(
        "s3",
        endpoint_url=f"{scheme}://{endpoint}",
        aws_access_key_id=os.environ[f"{prefix}_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ[f"{prefix}_S3_SECRET_KEY"],
        aws_session_token=os.environ.get(f"{prefix}_S3_SESSION_TOKEN") or None,
        region_name=os.environ.get(f"{prefix}_S3_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def s3_split(s3_path):
    """Split 's3://bucket/path/to/file' into (bucket, key)."""
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
    log(f"Reading {s3_path}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def write_parquet(s3, df, s3_path, presigned_url=None):
    log(f"Writing {len(df)} rows to {s3_path}")
    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    if presigned_url:
        resp = requests.put(presigned_url, data=buf.getvalue(), timeout=300)
        resp.raise_for_status()
        return
    bucket, key = s3_split(s3_path)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def main():
    # ── Parse context ───────────────────────────────────────────────
    ctx = json.loads(os.environ["NODE_CONTEXT"])
    config  = ctx["config"]
    inputs  = ctx["inputs"]
    output  = ctx["output"]
    node    = ctx["node"]

    cols_raw      = config.get("columns", "").strip()
    keep_original = config.get("keep_original", True)

    log(f"=== Label Encoder: {node['name']} ===")
    log(f"Columns: {cols_raw}")
    log(f"Keep originals: {keep_original}")

    if not cols_raw:
        log_error("'columns' config is required — provide comma-separated column names")
        sys.exit(1)

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

    # ── Read input ──────────────────────────────────────────────────
    input_entry = inputs[0]["output"]
    input_path = input_entry["path"]
    df = read_parquet(client_for(input_path), input_path, input_entry.get("presignedUrl"))
    log(f"Loaded {len(df)} rows, {len(df.columns)} columns: {list(df.columns)}")

    # ── Resolve columns ─────────────────────────────────────────────
    cols = [c.strip() for c in cols_raw.split(",") if c.strip()]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        log_error(f"Columns not found in dataset: {missing}")
        sys.exit(1)

    # ── Encode ──────────────────────────────────────────────────────
    encoding_map = {}  # for logging: col -> {value: label}

    for col in cols:
        le = LabelEncoder()

        # Fill nulls with a placeholder so LabelEncoder doesn't crash
        col_data = df[col].fillna("__missing__").astype(str)
        encoded = le.fit_transform(col_data)

        encoded_col = f"{col}_encoded"
        df[encoded_col] = encoded

        mapping = {str(cls): int(idx) for idx, cls in enumerate(le.classes_)}
        encoding_map[col] = mapping
        log(f"Encoded '{col}' -> '{encoded_col}': {mapping}")

        if not keep_original:
            df.drop(columns=[col], inplace=True)
            log(f"Dropped original column '{col}'")

    # ── Write output ────────────────────────────────────────────────
    out_entry = output["files"][0]
    out_path = out_entry["path"]
    write_parquet(artifact_client, df, out_path, out_entry.get("presignedUrl"))

    print(json.dumps({
        "success": True,
        "nodeName": node["name"],
        "rowCount": len(df),
        "encodedColumns": cols,
        "encodingMap": encoding_map,
        "keepOriginal": keep_original,
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
