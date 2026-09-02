#!/usr/bin/env python3
"""
Sleep Node - Custom Node Container

Sleeps for a configured number of seconds, then passes its input artifact
through to its output path unchanged (byte-for-byte copy). Wire it between
two nodes to delay the downstream stage — useful for testing slow pipeline
stages, execution timeouts, and short-lived STS credential expiry.

Follows the NodeContext contract - receives a single NODE_CONTEXT JSON.

The input is always re-read at read time via storage_v2 (POST /api/node/
storage/download-url), which resolves the source's CURRENT version regardless
of which bucket it lives in — race-free for an upstream node that wrote its
output in this same run. Output is written via the platform's presigned PUT
URL when attached, falling back to boto3 + ARTIFACT_S3_* credentials:
    *_ENDPOINT       - S3 endpoint (host:port, no scheme)
    *_ACCESS_KEY     - STS access key
    *_SECRET_KEY     - STS secret key
    *_SESSION_TOKEN  - STS session token (optional)
    *_BUCKET         - Bucket name
    *_USE_SSL        - Use SSL for S3 (default: false)
    *_REGION         - S3 region (default: us-east-1)
"""

import json
import os
import sys
import time

import storage_boto3 as boto3
import requests
from botocore.client import Config
from storage_v2 import read as storage_read

# Bump this on every code change so a run's logs prove which build is live.
NODE_VERSION = "2026-07-14.1"


def log(msg):
    print(f"[SLEEP] {msg}", flush=True)


def log_error(msg):
    print(f"[SLEEP ERROR] {msg}", file=sys.stderr, flush=True)


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


def parse_duration(raw):
    """Validate the sleep duration. Must be a non-negative number of seconds."""
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        raise ValueError("duration_seconds is required")
    try:
        seconds = float(str(raw).strip())
    except (TypeError, ValueError):
        raise ValueError(f"Invalid duration_seconds: {raw!r} (must be a non-negative number)")
    if seconds < 0:
        raise ValueError(f"duration_seconds must be >= 0, got {seconds}")
    return seconds


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


def sleep_with_heartbeat(seconds):
    """Sleep in <=60s chunks, logging progress so the execution log shows liveness."""
    deadline = time.monotonic() + seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 60))
        remaining = deadline - time.monotonic()
        if remaining > 0:
            log(f"Sleeping... {remaining:.0f}s remaining")


def main():
    log(f"version {NODE_VERSION}")
    ctx = parse_context()
    node = ctx.get("node", {})

    try:
        config = ctx["config"]
        inputs = ctx.get("inputs") or []
        out_files = ctx["output"]["files"]

        duration = parse_duration(config.get("duration_seconds"))

        if not inputs:
            raise ValueError("Sleep node requires one upstream input to pass through")
        src_entry = inputs[0]["output"]
        src_path = src_entry["path"]

        log(f"Node: {node['name']} | Sleep: {duration:.0f}s | Passthrough: {src_path}")

        if duration > 900:
            log("WARNING: S3 credentials are short-lived STS tokens (900-3600s); "
                "sleeps longer than the token lifetime will fail at the copy step")

        t0 = time.monotonic()
        sleep_with_heartbeat(duration)
        log(f"Slept {time.monotonic() - t0:.1f}s; copying input through")

        # Some S3-compatible endpoints (older MinIO/Ceph) reject the request
        # checksums newer botocore adds by default. Only send them when the
        # operation actually requires it.
        os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
        os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

        artifact_s3 = get_s3_client("ARTIFACT")

        # Always re-resolve the source at read time via storage_v2 (POST
        # /api/node/storage/download-url) instead of trusting
        # src_entry["presignedUrl"] or doing a raw boto3 copy_object/
        # download_file keyed off src_path: that path is the LOGICAL path the
        # platform assigns, signed/resolved at workflow-submit time before an
        # upstream node in this same run has written anything, so it can point
        # at a key that doesn't exist yet and 404s. storage_read() fetches
        # fresh bytes against the file's CURRENT version, which is race-free
        # regardless of which bucket the source lives in.
        log(f"Reading {src_path} ...")
        data = storage_read(src_path)
        log(f"Read {len(data)} bytes; writing through")

        # Write to the exact path the platform assigned (always artifact bucket).
        main_out = out_files[0]
        main_presigned_url = main_out.get("presignedUrl")
        dest_bucket, dest_key = split_s3(main_out["path"])

        if main_presigned_url:
            log(f"Uploading -> {main_out['path']} via presigned URL")
            resp = requests.put(main_presigned_url, data=data, timeout=300)
            resp.raise_for_status()
        else:
            log(f"Uploading -> s3://{dest_bucket}/{dest_key}")
            artifact_s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=data)

        # Additional outputs share the same already-fetched bytes.
        for f in out_files[1:]:
            extra_presigned_url = f.get("presignedUrl")
            if extra_presigned_url:
                log(f"Uploading -> {f['path']} via presigned URL (extra output)")
                resp = requests.put(extra_presigned_url, data=data, timeout=300)
                resp.raise_for_status()
            else:
                _, extra_key = split_s3(f["path"])
                log(f"Uploading -> s3://{dest_bucket}/{extra_key} (extra output)")
                artifact_s3.put_object(Bucket=dest_bucket, Key=extra_key, Body=data)

        log(f"Completed OK (version {NODE_VERSION})")
        print(json.dumps({
            "success": True,
            "version": NODE_VERSION,
            "nodeName": node["name"],
            "sleptSeconds": round(time.monotonic() - t0, 1),
            "source": src_path,
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
