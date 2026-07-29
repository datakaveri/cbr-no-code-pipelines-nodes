#!/usr/bin/env python3
"""
DB Table Source - Custom Node Container

Uses the cbr-data-access SDK to download a table from the caller's approved
access request on the CBR data-access server, and upload the result as
Parquet to MinIO so downstream pipeline nodes can consume it as an artifact.

Authentication reuses the caller's own identity instead of asking for
credentials in the node config: Argo injects API_KEY into every node pod (see
the "Auth Check" node), so this node hands that key to the SDK. The server
resolves it to the user who ran the pipeline. The key does not expire
mid-run and is revoked when the execution finishes, so there is nothing to
refresh.

This node has no S3 inputs (the table comes from the data-access server via
the SDK), so it only needs the artifact bucket's credentials:
  ARTIFACT_S3_*  - artifact bucket (workflow outputs), read+write
Writes to the exact paths the platform assigns in output.files[*].path.
"""

import json
import os
import re
import sys
import tempfile
import time

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from cbr_data_access import DataAccessClient
from cbr_data_access.exceptions import AuthenticationError, DataAccessError

# Bump this on every code change so a run's logs prove which build is live.
NODE_VERSION = "2026-07-15.1-api-key-auth"


def log(msg):
    print(f"[DB-TABLE-SOURCE] {msg}", flush=True)


def log_error(msg):
    print(f"[DB-TABLE-SOURCE ERROR] {msg}", file=sys.stderr, flush=True)


def parse_context():
    raw = os.environ.get("NODE_CONTEXT", "")
    if not raw:
        raise ValueError("NODE_CONTEXT is required")
    return json.loads(raw)


def s3_key_from_path(s3_path):
    # s3://bucket/some/key  →  some/key
    return s3_path.replace("s3://", "").split("/", 1)[1]


def parse_row_limit(raw):
    """Validate the optional row cap. Blank/None/0 means no limit (full table)."""
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return None
    try:
        n = int(str(raw).strip())
    except (TypeError, ValueError):
        raise ValueError(f"Invalid row_limit: {raw!r} (must be a positive integer)")
    if n < 0:
        raise ValueError(f"row_limit must be >= 0, got {n}")
    return n or None  # 0 => no limit


def build_authenticated_client(**sdk_kwargs):
    """Build a DataAccessClient from the API key Argo injects into the pod,
    instead of credentials stored in the pipeline config."""
    api_key = os.environ.get("API_KEY")
    if not api_key:
        raise AuthenticationError("API_KEY is not set in this pod")

    return DataAccessClient(api_key=api_key, **sdk_kwargs)


def main():
    log(f"version {NODE_VERSION}")
    ctx = parse_context()
    config   = ctx["config"]
    node     = ctx["node"]
    output   = ctx["output"]

    table_name = config["table_name"].strip()
    view_id    = config.get("view_id", "").strip() or None
    row_limit  = parse_row_limit(config.get("row_limit"))
    out_files  = output["files"]

    log(f"Node: {node['name']} | Table: {table_name} | "
        f"View ID: {view_id or 'auto'} | Limit: {row_limit or 'none'}")

    # Optional endpoint overrides (pass as pod env vars if needed)
    sdk_kwargs = {}
    if os.environ.get("CBR_KEYCLOAK_URL"):
        sdk_kwargs["keycloak_url"] = os.environ["CBR_KEYCLOAK_URL"]
    if os.environ.get("CBR_BASE_URL"):
        sdk_kwargs["base_url"] = os.environ["CBR_BASE_URL"]

    # Some S3-compatible endpoints (older MinIO/Ceph) reject the request
    # checksums newer botocore adds by default. Only send them when the
    # operation actually requires it. The env form is ignored by botocore
    # versions that predate the option, so it is safe across boto3 versions.
    os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
    os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

    s3 = boto3.client(
        "s3",
        endpoint_url="{}://{}".format(
            "https" if os.environ.get("ARTIFACT_S3_USE_SSL", "false").lower() == "true" else "http",
            os.environ["ARTIFACT_S3_ENDPOINT"],
        ),
        aws_access_key_id=os.environ["ARTIFACT_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["ARTIFACT_S3_SECRET_KEY"],
        aws_session_token=os.environ.get("ARTIFACT_S3_SESSION_TOKEN") or None,
        region_name=os.environ.get("ARTIFACT_S3_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )
    bucket = os.environ["ARTIFACT_S3_BUCKET"]

    try:
        with build_authenticated_client(**sdk_kwargs) as client:
            # Resolved server-side: the key is opaque, so this both names the
            # user and proves the key is live before we query.
            log(f"Authenticated with injected API key as: {client.identity}")

            if view_id:
                log(f"Setting access request: {view_id}")
                client.set_access(view_id)

            # query() interpolates table_name into SQL via auto-qualification.
            # node.json's contract is a bare table name, so validate it as a
            # plain identifier first.
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
                raise ValueError(f"Invalid table_name: {table_name!r}")

            sql = f"SELECT * FROM {table_name}"  # auto-qualified by the SDK
            if row_limit:
                sql += f" LIMIT {row_limit}"     # row_limit is a validated int
            first_key = s3_key_from_path(out_files[0]["path"])

            # Parquet codec. snappy is ~5-10x cheaper to encode than zstd and
            # the output stays small. Override with PARQUET_COMPRESSION
            # (e.g. "zstd", "none").
            compression = os.environ.get("PARQUET_COMPRESSION", "snappy").strip().lower()
            if compression in ("", "none"):
                compression = None
            log(f"Parquet compression: {compression or 'none'}")

            with tempfile.TemporaryDirectory() as tmp:
                local_path = os.path.join(tmp, f"{out_files[0]['name']}.parquet")

                # Phase 1: fetch the (row-capped) result set as a single
                # DataFrame, then encode Parquet once. The row limit keeps this
                # memory-bounded, and read/encode are no longer interleaved, so
                # the encode never back-pressures the DB read.
                t0 = time.monotonic()
                log(f"Querying: {sql}")
                df = client.query(sql)
                log(f"DB read complete: {len(df):,} rows in "
                    f"{time.monotonic() - t0:.1f}s; encoding Parquet")

                t_enc = time.monotonic()
                df.to_parquet(local_path, index=False, compression=compression)
                size_mb = os.path.getsize(local_path) / 1e6
                log(f"Parquet written: {len(df):,} rows, {size_mb:.1f} MB on disk in "
                    f"{time.monotonic() - t_enc:.1f}s "
                    f"(DB+encode total {time.monotonic() - t0:.1f}s); uploading next")

                # Phase 2: upload the seekable file.
                # This S3 endpoint doesn't support multipart uploads, so force a
                # single PUT by raising the threshold above any realistic size.
                # upload_file streams the body straight from disk, so even a large
                # single PUT never buffers the whole table in memory.
                t1 = time.monotonic()
                transfer = TransferConfig(
                    multipart_threshold=5 * 1024 * 1024 * 1024,  # 5 GiB: never split
                    use_threads=False,
                )
                log(f"Uploading {size_mb:.1f} MB -> s3://{bucket}/{first_key} ...")
                s3.upload_file(local_path, bucket, first_key, Config=transfer)
                dt = time.monotonic() - t1
                log(f"Upload done in {dt:.1f}s ({size_mb / max(dt, 1e-3):.1f} MB/s)")

                # Additional outputs share the same bytes: single-request
                # server-side copy (copy_object, not the managed multipart copy).
                for f in out_files[1:]:
                    extra_key = s3_key_from_path(f["path"])
                    log(f"Copying to s3://{bucket}/{extra_key} ...")
                    s3.copy_object(
                        Bucket=bucket,
                        CopySource={"Bucket": bucket, "Key": first_key},
                        Key=extra_key,
                    )

        log(f"Completed OK (version {NODE_VERSION})")
        print(json.dumps({
            "success": True,
            "version": NODE_VERSION,
            "nodeName": node["name"],
            "tableName": table_name,
            "outputs": [
                {"name": f["name"], "path": f["path"]}
                for f in out_files
            ],
        }))

    except DataAccessError as e:
        log_error(str(e))
        print(json.dumps({"success": False, "nodeName": node["name"], "error": str(e)}))
        sys.exit(1)
    except Exception as e:
        log_error(str(e))
        print(json.dumps({"success": False, "nodeName": node["name"], "error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
