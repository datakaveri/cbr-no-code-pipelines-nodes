#!/usr/bin/env python3
"""
STL Merger - Custom Node Container

Scaffold container for an STL merger node. It follows the same NODE_CONTEXT
pattern as the transformer image, but does not require upstream inputs.
For now it writes a minimal valid STL artifact to the resolved output path.

This node has no S3 inputs, so it only needs the artifact bucket's
credentials:
  ARTIFACT_S3_*  - artifact bucket (workflow outputs), read+write
Writes to the exact paths the platform assigns in output.files[*].path.
"""

import json
import os
import sys
from urllib.parse import urlparse

import storage_boto3 as boto3
import requests


def log(message: str) -> None:
    print(f"[STL_MERGER] {message}", flush=True)


def log_error(message: str) -> None:
    print(f"[STL_MERGER ERROR] {message}", file=sys.stderr, flush=True)


def parse_node_context() -> dict:
    ctx_str = os.environ.get("NODE_CONTEXT", "")
    if not ctx_str:
        raise ValueError("NODE_CONTEXT environment variable is required")

    try:
        return json.loads(ctx_str)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Failed to parse NODE_CONTEXT: {exc}") from exc


def validate_context(ctx: dict) -> None:
    if not ctx.get("node", {}).get("name"):
        raise ValueError("node.name is required in NODE_CONTEXT")

    output_files = ctx.get("output", {}).get("files", [])
    if not output_files:
        raise ValueError("output.files is required in NODE_CONTEXT")


def create_s3_client():
    use_ssl = os.environ.get("ARTIFACT_S3_USE_SSL", "false").lower() == "true"
    endpoint = os.environ["ARTIFACT_S3_ENDPOINT"]
    endpoint_url = f"{'https' if use_ssl else 'http'}://{endpoint}"

    client_kwargs = {
        "service_name": "s3",
        "endpoint_url": endpoint_url,
        "aws_access_key_id": os.environ["ARTIFACT_S3_ACCESS_KEY"],
        "aws_secret_access_key": os.environ["ARTIFACT_S3_SECRET_KEY"],
        "region_name": os.environ.get("ARTIFACT_S3_REGION", "us-east-1"),
    }

    session_token = os.environ.get("ARTIFACT_S3_SESSION_TOKEN", "")
    if session_token:
        client_kwargs["aws_session_token"] = session_token

    return boto3.client(**client_kwargs)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path:
        raise ValueError(f"Invalid S3 URI: {uri}")

    return parsed.netloc, parsed.path.lstrip("/")


def build_ascii_stl(node_name: str, output_name: str) -> bytes:
    solid_name = f"{node_name}_{output_name}".replace(" ", "_")
    stl_text = f"""solid {solid_name}
  facet normal 0 0 1
    outer loop
      vertex 0 0 0
      vertex 1 0 0
      vertex 0 1 0
    endloop
  endfacet
endsolid {solid_name}
"""
    return stl_text.encode("utf-8")


def main() -> None:
    ctx = parse_node_context()
    validate_context(ctx)

    node = ctx["node"]
    output = ctx["output"]
    node_name = node["name"]
    node_slug = node.get("slug", "stl-merger")
    output_files = output["files"]

    log("=== STL Merger Node ===")
    log(f"Node: {node_name} ({node_slug})")
    log(f"Inputs received: {len(ctx.get('inputs', []))}")
    log(f"Outputs: {len(output_files)}")

    s3_client = create_s3_client()

    try:
        uploaded_outputs = []
        for output_file in output_files:
            output_name = output_file.get("name", "merged_model")
            output_format = output_file.get("format", "stl")
            output_path = output_file["path"]

            if output_format.lower() != "stl":
                raise ValueError(f"Unsupported output format for STL merger: {output_format}")

            body = build_ascii_stl(node_name, output_name)

            # Prefer the platform's presigned PUT URL when attached to this
            # output; fall back to boto3 + ARTIFACT_S3_* credentials otherwise
            # (dual mode during the STS-to-presigned migration).
            presigned_url = output_file.get("presignedUrl")
            if presigned_url:
                log(f"Uploading '{output_name}' to {output_path} via presigned URL...")
                resp = requests.put(presigned_url, data=body, timeout=300)
                resp.raise_for_status()
            else:
                bucket, key = parse_s3_uri(output_path)
                log(f"Uploading '{output_name}' to {output_path}...")
                s3_client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=body,
                    ContentType="model/stl",
                )

            uploaded_outputs.append(
                {
                    "name": output_name,
                    "path": output_path,
                    "format": output_format,
                }
            )

        log("=== STL Merger Node Complete ===")
        print(
            json.dumps(
                {
                    "success": True,
                    "nodeName": node_name,
                    "outputs": uploaded_outputs,
                }
            )
        )
    except Exception as exc:
        log_error(f"Failed: {exc}")
        print(
            json.dumps(
                {
                    "success": False,
                    "nodeName": node_name,
                    "error": str(exc),
                }
            )
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
