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


def log(msg):
    print(f"[ENCODER] {msg}", flush=True)


def log_error(msg):
    print(f"[ENCODER ERROR] {msg}", file=sys.stderr, flush=True)


def read_parquet(s3_path):
    log(f"Reading {s3_path}")
    return pd.read_parquet(BytesIO(read(s3_path)))


def write_parquet(df, s3_path):
    buf = BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    with open_write(s3_path, content_type="application/vnd.apache.parquet") as out:
        out.write(buf.getvalue())


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

    # ── Read input ──────────────────────────────────────────────────
    input_entry = inputs[0]["output"]
    input_path = input_entry["path"]
    df = read_parquet(input_path)
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
    write_parquet(df, out_path)

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
