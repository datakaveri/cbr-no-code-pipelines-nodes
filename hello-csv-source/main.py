#!/usr/bin/env python3
"""
Hello CSV Source Node - Custom Node Container

Generates a configurable number of synthetic patient-vitals rows and writes
them as a CSV to the artifact bucket. A source node (no upstream input), so
it is the fastest end-to-end smoke test of a platform install: build, pull,
STS credentials, and artifact storage are all exercised by one tiny node.

Follows the NodeContext contract - receives a single NODE_CONTEXT JSON.

Resource Environment Variables (two buckets, one credential set each):
  INPUT_S3_*         - Upload bucket (user source files), read-only (unused here)
  ARTIFACT_S3_*      - Artifact bucket (workflow outputs), read+write
    *_ENDPOINT       - S3 endpoint (host:port, no scheme)
    *_ACCESS_KEY     - STS access key
    *_SECRET_KEY     - STS secret key
    *_SESSION_TOKEN  - STS session token (optional)
    *_BUCKET         - Bucket name
    *_USE_SSL        - Use SSL for S3 (default: false)
    *_REGION         - S3 region (default: us-east-1)
"""

import csv
import io
import json
import os
import random
import sys

import boto3
from botocore.client import Config

# Bump this on every code change so a run's logs prove which build is live.
NODE_VERSION = "2026-07-31.1"

FIRST_NAMES = [
    "Asha", "Ravi", "Meera", "Arjun", "Divya", "Kiran", "Nisha", "Vikram",
    "Priya", "Sanjay", "Lata", "Mohan", "Anita", "Rahul", "Sneha", "Deepak",
]


def log(msg):
    print(f"[HELLO CSV] {msg}", flush=True)


def log_error(msg):
    print(f"[HELLO CSV ERROR] {msg}", file=sys.stderr, flush=True)


def parse_context():
    raw = os.environ.get("NODE_CONTEXT", "")
    if not raw:
        raise ValueError("NODE_CONTEXT is required")
    return json.loads(raw)


def split_s3(s3_path):
    # s3://bucket/some/key  →  ("bucket", "some/key")
    rest = s3_path[len("s3://"):] if s3_path.startswith("s3://") else s3_path
    bucket, _, key = rest.partition("/")
    return bucket, key


def parse_rows(raw):
    """Validate the row count. Must be an integer in [1, 100000]."""
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        raise ValueError("rows is required")
    try:
        rows = int(float(str(raw).strip()))
    except (TypeError, ValueError):
        raise ValueError(f"Invalid rows: {raw!r} (must be an integer)")
    if not 1 <= rows <= 100000:
        raise ValueError(f"rows must be between 1 and 100000, got {rows}")
    return rows


def get_s3_client(prefix):
    """Build a boto3 client from the {prefix}_S3_* env vars (INPUT or ARTIFACT)."""
    endpoint = os.environ[f"{prefix}_S3_ENDPOINT"]
    use_ssl = os.environ.get(f"{prefix}_S3_USE_SSL", "false").lower() == "true"
    if "://" not in endpoint:
        endpoint = ("https://" if use_ssl else "http://") + endpoint
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ[f"{prefix}_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ[f"{prefix}_S3_SECRET_KEY"],
        aws_session_token=os.environ.get(f"{prefix}_S3_SESSION_TOKEN") or None,
        region_name=os.environ.get(f"{prefix}_S3_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def generate_csv(rows):
    """Return the synthetic dataset as CSV bytes."""
    rng = random.Random(42)  # deterministic: same config -> same output
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["patient_id", "name", "age", "heart_rate_bpm", "temperature_c"])
    for i in range(1, rows + 1):
        writer.writerow([
            f"P{i:05d}",
            rng.choice(FIRST_NAMES),
            rng.randint(18, 90),
            rng.randint(55, 110),
            round(rng.uniform(36.0, 39.5), 1),
        ])
    return buf.getvalue().encode("utf-8")


def main():
    log(f"version {NODE_VERSION}")
    ctx = parse_context()
    node = ctx.get("node", {})

    try:
        config = ctx["config"]
        out_files = ctx["output"]["files"]

        rows = parse_rows(config.get("rows"))
        log(f"Node: {node['name']} | Generating {rows} rows")

        data = generate_csv(rows)
        log(f"Generated {len(data)} bytes of CSV")

        # Some S3-compatible endpoints (older MinIO/Ceph) reject the request
        # checksums newer botocore adds by default. Only send them when the
        # operation actually requires it.
        os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
        os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

        artifact_s3 = get_s3_client("ARTIFACT")

        # Write to the exact path the platform assigned (always artifact bucket).
        dest_bucket, dest_key = split_s3(out_files[0]["path"])
        log(f"Uploading -> s3://{dest_bucket}/{dest_key} ...")
        artifact_s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=data)

        # Additional outputs share the same bytes: single-request server-side copy.
        for f in out_files[1:]:
            _, extra_key = split_s3(f["path"])
            log(f"Copying to s3://{dest_bucket}/{extra_key} ...")
            artifact_s3.copy_object(
                Bucket=dest_bucket,
                CopySource={"Bucket": dest_bucket, "Key": dest_key},
                Key=extra_key,
            )

        log(f"Completed OK (version {NODE_VERSION})")
        print(json.dumps({
            "success": True,
            "version": NODE_VERSION,
            "nodeName": node["name"],
            "rows": rows,
            "outputs": [
                {"name": f["name"], "path": f["path"]}
                for f in out_files
            ],
        }))

    except Exception as e:
        log_error(str(e))
        print(json.dumps({"success": False, "nodeName": node.get("name"), "error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
