#!/usr/bin/env python3
"""
Feature Scaler Node
Scales numeric columns using StandardScaler or MinMaxScaler from scikit-learn.

Reads NODE_CONTEXT from environment (same contract as all other nodes).

Prefers the presigned GET/PUT URL the platform attaches to each input/output
entry in NODE_CONTEXT (`presignedUrl`) — plain HTTPS, no S3 credentials
needed. Falls back to boto3 + the INPUT_S3_*/ARTIFACT_S3_* credential env
vars when an entry has no presignedUrl, for compatibility with a backend
that predates the presigned-URL migration (dual mode during the rollout).
"""

import os
import sys
import json
from io import BytesIO

import pandas as pd
import requests
from storage_v2 import open_write, read
from sklearn.preprocessing import StandardScaler, MinMaxScaler


def log(msg):
    print(f"[SCALER] {msg}", flush=True)


def log_error(msg):
    print(f"[SCALER ERROR] {msg}", file=sys.stderr, flush=True)


def read_parquet(s3_path):
    log(f"Reading {s3_path}")
    return pd.read_parquet(BytesIO(read(s3_path)))


def write_parquet(df, s3_path):
    log(f"Writing {len(df)} rows to {s3_path}")
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

    method     = config.get("method", "standard")
    cols_raw   = config.get("columns", "").strip()

    log(f"=== Feature Scaler: {node['name']} ===")
    log(f"Method: {method}")
    log(f"Columns config: '{cols_raw or '(all numeric)'}'")

    if not inputs:
        log_error("No inputs provided")
        sys.exit(1)

    # ── Read input ──────────────────────────────────────────────────
    input_entry = inputs[0]["output"]
    input_path = input_entry["path"]
    df = read_parquet(input_path)
    log(f"Loaded {len(df)} rows, {len(df.columns)} columns: {list(df.columns)}")

    # ── Resolve columns to scale ────────────────────────────────────
    if cols_raw:
        cols = [c.strip() for c in cols_raw.split(",") if c.strip()]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            log_error(f"Columns not found in dataset: {missing}")
            sys.exit(1)
        # Only keep numeric ones from the user's list
        non_numeric = [c for c in cols if not pd.api.types.is_numeric_dtype(df[c])]
        if non_numeric:
            log_error(f"Cannot scale non-numeric columns: {non_numeric}")
            sys.exit(1)
    else:
        # Auto-detect all numeric columns
        cols = df.select_dtypes(include="number").columns.tolist()
        if not cols:
            log_error("No numeric columns found in dataset")
            sys.exit(1)
        log(f"Auto-selected numeric columns: {cols}")

    # ── Scale ───────────────────────────────────────────────────────
    scaler = StandardScaler() if method == "standard" else MinMaxScaler()
    df[cols] = scaler.fit_transform(df[cols])
    log(f"Scaled {len(cols)} columns: {cols}")

    # ── Write output ────────────────────────────────────────────────
    out_entry = output["files"][0]
    out_path = out_entry["path"]
    write_parquet(df, out_path)

    print(json.dumps({
        "success": True,
        "nodeName": node["name"],
        "rowCount": len(df),
        "scaledColumns": cols,
        "method": method,
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
