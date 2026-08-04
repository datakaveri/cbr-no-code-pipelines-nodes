#!/usr/bin/env python3
"""
3 D Merger - batch worker for Argo (no Streamlit server).

Reads NODE_CONTEXT, downloads upstream STL paths from S3, merges meshes with vedo,
uploads merged STL to the node's artifact output path, then exits.

Environment: NODE_CONTEXT plus two S3 credential sets (same contract as
sql-transformer):
  INPUT_S3_*     - upload bucket (user source files), read-only
  ARTIFACT_S3_*  - artifact bucket (workflow outputs), read+write
Inputs may live in either bucket; the client is chosen per path by its bucket.
Outputs always go to the artifact bucket.

Prefers the presigned GET/PUT URL the platform attaches to each input/output
entry in NODE_CONTEXT (`presignedUrl`) — plain HTTPS, no S3 credentials
needed. Falls back to boto3 + the INPUT_S3_*/ARTIFACT_S3_* credential env
vars when an entry has no presignedUrl (dual mode during the rollout).
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError
from vedo import load, merge, write


def log(msg: str) -> None:
    print(f"[3 D MERGER] {msg}", flush=True)


def log_err(msg: str) -> None:
    print(f"[3 D MERGER ERROR] {msg}", file=sys.stderr, flush=True)


def parse_node_context() -> dict:
    raw = os.environ.get("NODE_CONTEXT", "")
    if not raw:
        raise ValueError("NODE_CONTEXT is required")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid NODE_CONTEXT JSON: {e}") from e


def make_s3_client(prefix: str):
    """Build a boto3 S3 client from {prefix}_S3_* env vars (INPUT or ARTIFACT)."""
    endpoint = os.environ[f"{prefix}_S3_ENDPOINT"]
    use_ssl = os.environ.get(f"{prefix}_S3_USE_SSL", "false").lower() == "true"
    if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
        scheme = "https" if use_ssl else "http"
        endpoint = f"{scheme}://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ[f"{prefix}_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ[f"{prefix}_S3_SECRET_KEY"],
        aws_session_token=os.environ.get(f"{prefix}_S3_SESSION_TOKEN") or None,
        region_name=os.environ.get(f"{prefix}_S3_REGION", "us-east-1"),
    )


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {uri[:120]}")
    rest = uri[5:]
    slash = rest.find("/")
    if slash == -1:
        return rest, ""
    return rest[:slash], rest[slash + 1 :]


def main() -> None:
    ctx = parse_node_context()
    node = ctx.get("node") or {}
    inputs = ctx.get("inputs") or []
    output = ctx.get("output") or {}
    out_files = output.get("files") or []

    node_name = node.get("name", "node")
    node_slug = node.get("slug", "3-d-merger")

    log(f"=== 3 D merger ({node_slug}) ===")
    log(f"Node name: {node_name}")

    if not out_files:
        raise ValueError("output.files is required in NODE_CONTEXT")
    if not inputs:
        raise ValueError(
            "No upstream inputs in NODE_CONTEXT. Connect a Data Source that selects an STL file "
            "to this node."
        )

    # Two clients: inputs may come from either bucket; outputs always go to the
    # artifact bucket. Pick the client per path by the bucket in its s3:// URI.
    input_client = make_s3_client("INPUT")
    artifact_client = make_s3_client("ARTIFACT")
    artifact_bucket = os.environ["ARTIFACT_S3_BUCKET"]

    def client_for(uri: str):
        b, _ = parse_s3_uri(uri)
        return artifact_client if b == artifact_bucket else input_client

    stl_local_paths: list[str] = []
    root = tempfile.mkdtemp(prefix="3_d_merger_")
    try:
        for i, inp in enumerate(inputs):
            out = inp.get("output") or {}
            path = out.get("path")
            if not path:
                raise ValueError(f"Missing input path for {inp.get('nodeName', i)}")
            local = os.path.join(root, f"in_{i}.stl")
            presigned_url = out.get("presignedUrl")
            if presigned_url:
                log(f"Downloading {path} -> {local} via presigned URL")
                resp = requests.get(presigned_url, timeout=300)
                resp.raise_for_status()
                with open(local, "wb") as fh:
                    fh.write(resp.content)
            else:
                b, key = parse_s3_uri(path)
                log(f"Downloading {path} -> {local}")
                client_for(path).download_file(b, key, local)
            stl_local_paths.append(local)

        log(f"Merging {len(stl_local_paths)} mesh(es)...")
        meshes = [load(p) for p in stl_local_paths]
        merged = merge(meshes)

        # Write to the exact path the platform assigned (always artifact bucket).
        out_def = out_files[0]
        dest_uri = out_def["path"]
        _, dkey = parse_s3_uri(dest_uri)

        # vedo infers the writer from the filename extension; match the dest's.
        ext = os.path.splitext(dkey)[1].lstrip(".") or "stl"
        merged_local = os.path.join(root, f"merged.{ext}")
        write(merged, merged_local)

        out_presigned_url = out_def.get("presignedUrl")
        log(f"Uploading merged mesh to {dest_uri}")
        if out_presigned_url:
            with open(merged_local, "rb") as fh:
                resp = requests.put(out_presigned_url, data=fh.read(), timeout=300)
            resp.raise_for_status()
        else:
            db, _ = parse_s3_uri(dest_uri)
            client_for(dest_uri).upload_file(merged_local, db, dkey)

        result = {
            "success": True,
            "nodeName": node_name,
            "inputs": len(stl_local_paths),
            "outputPath": dest_uri,
            "format": out_def.get("format", ext),
        }
        print(json.dumps(result), flush=True)
        log("Done.")
    except (BotoCoreError, ClientError) as e:
        log_err(f"S3 operation failed: {e}")
        print(json.dumps({"success": False, "error": "S3 operation failed"}), flush=True)
        sys.exit(1)
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_err(str(e))
        print(json.dumps({"success": False, "error": str(e)}), flush=True)
        sys.exit(1)
