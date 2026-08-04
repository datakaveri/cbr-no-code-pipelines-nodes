#!/usr/bin/env python3
"""
DB View Source - Custom Node Container

Uses the cbr-data-access SDK to authenticate with Keycloak, download a
specified view from the CBR data-access server as Parquet, and upload it
to S3 so downstream pipeline nodes can consume it as an artifact.

This node has no S3 inputs (the view comes from the data-access server via
the SDK), so it only needs the artifact bucket's credentials:
  ARTIFACT_S3_*  - artifact bucket (workflow outputs), read+write
Writes to the exact paths the platform assigns in output.files[*].path.

Prefers the presigned PUT URL the platform attaches to each output entry in
NODE_CONTEXT (`presignedUrl`) — plain HTTPS, no S3 credentials needed. Falls
back to boto3 + the ARTIFACT_S3_* credential env vars when an entry has no
presignedUrl (dual mode during the rollout).
"""

import json
import os
import sys
import tempfile

import boto3
import requests
from botocore.client import Config
from cbr_data_access import DataAccessClient
from cbr_data_access.exceptions import DataAccessError


def log(msg):
    print(f"[DB-VIEW-SOURCE] {msg}", flush=True)


def log_error(msg):
    print(f"[DB-VIEW-SOURCE ERROR] {msg}", file=sys.stderr, flush=True)


def parse_context():
    raw = os.environ.get("NODE_CONTEXT", "")
    if not raw:
        raise ValueError("NODE_CONTEXT is required")
    return json.loads(raw)


def s3_key_from_path(s3_path):
    # s3://bucket/some/key  →  some/key
    return s3_path.replace("s3://", "").split("/", 1)[1]


def main():
    ctx = parse_context()
    config   = ctx["config"]
    node     = ctx["node"]
    output   = ctx["output"]

    username  = config["username"]
    password  = config["password"]
    view_name = config["view_name"]
    out_files = output["files"]

    log(f"Node: {node['name']} | View: {view_name} | User: {username}")

    # Optional endpoint overrides (pass as pod env vars if needed)
    sdk_kwargs = {}
    if os.environ.get("CBR_KEYCLOAK_URL"):
        sdk_kwargs["keycloak_url"] = os.environ["CBR_KEYCLOAK_URL"]
    if os.environ.get("CBR_BASE_URL"):
        sdk_kwargs["base_url"] = os.environ["CBR_BASE_URL"]

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
        with DataAccessClient(username=username, password=password, **sdk_kwargs) as client:
            log("Authenticating with Keycloak...")
            client.login()
            log("Authenticated")

            with tempfile.TemporaryDirectory() as tmp:
                log(f"Downloading view '{view_name}' from CBR data-access server...")
                local_path = client.download_view(view_name, tmp)
                log(f"Downloaded → {local_path}")

                for f in out_files:
                    presigned_url = f.get("presignedUrl")
                    if presigned_url:
                        log(f"Uploading -> {f['path']} via presigned URL...")
                        with open(local_path, "rb") as fh:
                            resp = requests.put(presigned_url, data=fh.read(), timeout=300)
                        resp.raise_for_status()
                    else:
                        s3_key = s3_key_from_path(f["path"])
                        log(f"Uploading to s3://{bucket}/{s3_key} ...")
                        s3.upload_file(str(local_path), bucket, s3_key)
                    log("Upload complete")

        print(json.dumps({
            "success": True,
            "nodeName": node["name"],
            "viewName": view_name,
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
