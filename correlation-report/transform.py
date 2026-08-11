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
from storage_v2 import open_write, read


def log(msg):
    print(f"[CORRELATION] {msg}", flush=True)


def log_error(msg):
    print(f"[CORRELATION ERROR] {msg}", file=sys.stderr, flush=True)


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

    input_entry = inputs[0]["output"]
    input_path = input_entry["path"]
    df = read_parquet(input_path)
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
    write_parquet(result_df, out_path)

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
