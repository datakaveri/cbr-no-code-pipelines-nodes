#!/usr/bin/env python3
"""
Sleep Node - Custom Node Container

Sleeps for a configured number of seconds, then passes its input artifact
through to its output path unchanged (byte-for-byte copy). Wire it between
two nodes to delay the downstream stage — useful for testing slow pipeline
stages, execution timeouts, and short-lived STS credential expiry.

Follows the NodeContext contract - receives a single NODE_CONTEXT JSON.

Resource Environment Variables (two buckets, one credential set each):
  INPUT_S3_*         - Upload bucket (user source files), read-only
  ARTIFACT_S3_*      - Artifact bucket (workflow outputs), read+write
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
import tempfile
import time

import storage_boto3 as boto3
import requests
from boto3.s3.transfer import TransferConfig
from botocore.client import Config

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
        src_presigned_url = src_entry.get("presignedUrl")

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

        artifact_bucket = os.environ["ARTIFACT_S3_BUCKET"]
        artifact_s3 = get_s3_client("ARTIFACT")

        src_bucket, src_key = split_s3(src_path)
        # Write to the exact path the platform assigned (always artifact bucket).
        main_out = out_files[0]
        main_presigned_url = main_out.get("presignedUrl")
        dest_bucket, dest_key = split_s3(main_out["path"])

        # `data` is populated only when the presigned GET+PUT path below runs,
        # so extra outputs can reuse the already-fetched bytes instead of
        # re-downloading them.
        data = None

        if src_presigned_url and main_presigned_url:
            # Preferred: plain HTTPS through the platform's presigned URLs, no
            # S3 credentials needed on either end.
            log(f"Passthrough via presigned URLs: {src_path} -> {main_out['path']}")
            resp = requests.get(src_presigned_url, timeout=300)
            resp.raise_for_status()
            data = resp.content
            resp = requests.put(main_presigned_url, data=data, timeout=300)
            resp.raise_for_status()
        elif src_bucket == artifact_bucket:
            # Same bucket and credentials: single-request server-side copy.
            log(f"Server-side copy s3://{src_bucket}/{src_key} -> s3://{dest_bucket}/{dest_key}")
            artifact_s3.copy_object(
                Bucket=dest_bucket,
                CopySource={"Bucket": src_bucket, "Key": src_key},
                Key=dest_key,
            )
        else:
            # Cross-bucket (upload -> artifact): each bucket has its own
            # credentials, so a server-side copy can't span them — stream
            # through the pod instead.
            input_s3 = get_s3_client("INPUT")
            with tempfile.TemporaryDirectory() as tmp:
                local_path = os.path.join(tmp, "passthrough")
                log(f"Downloading s3://{src_bucket}/{src_key} ...")
                input_s3.download_file(src_bucket, src_key, local_path)
                size_mb = os.path.getsize(local_path) / 1e6

                # This S3 endpoint doesn't support multipart uploads, so force a
                # single PUT by raising the threshold above any realistic size.
                transfer = TransferConfig(
                    multipart_threshold=5 * 1024 * 1024 * 1024,  # 5 GiB: never split
                    use_threads=False,
                )
                log(f"Uploading {size_mb:.1f} MB -> s3://{dest_bucket}/{dest_key} ...")
                artifact_s3.upload_file(local_path, dest_bucket, dest_key, Config=transfer)

        # Additional outputs share the same bytes. There's no presigned
        # equivalent of copy_object: if the main step already pulled the bytes
        # into memory (presigned path above), reuse them for a direct PUT;
        # otherwise re-fetch via a fresh presigned GET of the source when both
        # ends have a presignedUrl, falling back to the original server-side
        # copy_object otherwise.
        for f in out_files[1:]:
            extra_presigned_url = f.get("presignedUrl")
            if extra_presigned_url and data is not None:
                log(f"Uploading -> {f['path']} via presigned URL (extra output)...")
                resp = requests.put(extra_presigned_url, data=data, timeout=300)
                resp.raise_for_status()
            elif extra_presigned_url and src_presigned_url:
                log(f"Uploading -> {f['path']} via presigned URL (extra output, re-fetched)...")
                resp = requests.get(src_presigned_url, timeout=300)
                resp.raise_for_status()
                resp = requests.put(extra_presigned_url, data=resp.content, timeout=300)
                resp.raise_for_status()
            else:
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
