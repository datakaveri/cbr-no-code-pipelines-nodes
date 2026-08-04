#!/usr/bin/env python3
"""
OMOP Wide Table Source - Custom Node Container

Exports a wide-format ("one row per subject-visit") view of the caller's
granted OMOP cohort data and uploads it as Parquet for downstream nodes.

The heavy lifting is the vendored SDK's aggregate(): it pulls the long-format
rows one cohort at a time, spools them to temporary Parquet parts, then dedupes
and pivots each local_concept.dataset off that spool with polars' streaming
engine. Peak memory is one dataset's wide frame, not the whole pull -- which is
what keeps a large cohort from OOMing in a pod with a ~2Gi limit.

aggregate() returns one wide frame per dataset, but a node declares its outputs
statically. Rather than make the user pick one dataset per node, this stacks
every granted dataset into a single result.parquet and adds a `dataset` column
naming each row's source:

    dataset | Barcode | Visit | aft_score | mmse_total
    AFT     |  B001   |  V1   |    1.0    |    null
    MMSE    |  B001   |  V1   |   null    |     28

Datasets don't share field columns, so the sheet is sparse by construction: a
row is null in the columns owned by the other datasets. That is the trade for
one file and no config -- downstream narrows it with a plain
`WHERE dataset = 'AFT'`.

Authentication reuses the caller's own identity instead of storing credentials
in the node config: Argo injects API_KEY into every node pod, this node writes
it to a token file (the only credential source the SDK reads), and the server
resolves it to the user who ran the pipeline. The key does not expire mid-run
and is revoked when the execution finishes.

This node has no S3 inputs (the data comes from the data-access server via the
SDK), so it only needs the artifact bucket's credentials:
  ARTIFACT_S3_*  - artifact bucket (workflow outputs), read+write
Writes to the exact paths the platform assigns in output.files[*].path.
"""

import json
import os
import sys
import tempfile
import time

import boto3
import polars as pl
import requests
from boto3.s3.transfer import TransferConfig
from botocore.client import Config
from cbr_data_access import DataAccessClient
from cbr_data_access.aggregate import aggregate
from cbr_data_access.exceptions import AuthenticationError, DataAccessError

# Bump this on every code change so a run's logs prove which build is live.
NODE_VERSION = "2026-07-23.1-cohorts-from-registry"

# The "All granted cohorts" row in the node.json select. A select option can't
# carry an empty value, so "every cohort" travels as this sentinel rather than
# as a blank.
ALL_COHORTS = "__all__"


def log(msg):
    print(f"[OMOP-WIDE-TABLE-SOURCE] {msg}", flush=True)


def log_error(msg):
    print(f"[OMOP-WIDE-TABLE-SOURCE ERROR] {msg}", file=sys.stderr, flush=True)


def parse_context():
    raw = os.environ.get("NODE_CONTEXT", "")
    if not raw:
        raise ValueError("NODE_CONTEXT is required")
    return json.loads(raw)


def s3_key_from_path(s3_path):
    # s3://bucket/some/key  →  some/key
    return s3_path.replace("s3://", "").split("/", 1)[1]


def parse_request_id(raw):
    """The access-request picker stores {request_id, use_case}; older configs
    (and hand-edited ones) may carry the bare id string."""
    if isinstance(raw, dict):
        return (raw.get("request_id") or "").strip() or None
    return (str(raw).strip() or None) if raw else None


def parse_cohort(raw):
    """The ALL_COHORTS sentinel, or blank, means every granted cohort.

    The select sends ALL_COHORTS when the user picks "All granted cohorts"; a
    node saved before that option existed sends nothing. Both mean the same to
    aggregate(), which reads None as its every-cohort default.
    """
    if raw is None or not str(raw).strip() or str(raw).strip() == ALL_COHORTS:
        return None
    # aggregate() accepts an id or a name and resolves names against the live
    # cohort_mappings registry before any heavy pull, raising ValueError with
    # the active set — no local validation to do here.
    return str(raw).strip()


def build_authenticated_client(**sdk_kwargs):
    """Build a DataAccessClient from the API key Argo injects into the pod,
    instead of credentials stored in the pipeline config.

    The SDK reads bearer tokens only from a file (token_file kwarg, else the
    platform sidecar's default path) -- it has no api_key argument -- so the
    injected key is written to a private token file and handed over by path.
    """
    api_key = os.environ.get("API_KEY")
    if not api_key:
        raise AuthenticationError("API_KEY is not set in this pod")

    token_file = os.path.join(tempfile.gettempdir(), ".cbr-token")
    fd = os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as fh:
        fh.write(api_key)

    return DataAccessClient(token_file=token_file, driver="adbc", **sdk_kwargs)


DATASET_COLUMN = "dataset"

# Identity columns aggregate() keys every wide frame by, in output order; a
# single-cohort export drops the constant Cohort column. Mirrors the SDK's
# private _INDEX_COLS — defined here so the vendored sdk/ stays an exact,
# unpatched copy of the reference SDK.
IDENTITY_COLUMNS = [
    "Barcode",
    "Subject_ID",
    "Gender",
    "Age_at_Visit",
    "Visit",
    "Assessment_Date",
]


def index_columns(cohort):
    return list(IDENTITY_COLUMNS) if cohort is not None else ["Cohort", *IDENTITY_COLUMNS]


def stack_datasets(frames, index_cols):
    """Stack every dataset's wide frame into one sheet, tagged by dataset.

    Each frame carries the same identity columns but its own field columns, so
    the stack is a diagonal concat: the result holds the union of all field
    columns, and a row is null in the columns belonging to the other datasets.
    The dataset column is what makes that readable -- it names the frame each
    row came from, so downstream can filter (WHERE dataset = 'AFT') rather than
    guess from which columns happen to be populated.

    Two datasets sharing a field_name land in one column here rather than
    colliding: same field, different rows, told apart by the dataset column.
    "diagonal_relaxed" is what allows that when they disagree on dtype -- it
    widens to a common supertype instead of raising.
    """
    tagged = [
        frame.with_columns(pl.lit(name).alias(DATASET_COLUMN))
        for name, frame in sorted(frames.items())
    ]
    stacked = pl.concat(tagged, how="diagonal_relaxed")
    return order_columns(stacked, index_cols)


def order_columns(frame, index_cols):
    """dataset first, then identity columns in aggregate's order, then fields."""
    lead = [DATASET_COLUMN] + [c for c in index_cols if c in frame.columns]
    ordered = frame.select(lead + sorted(c for c in frame.columns if c not in lead))
    # aggregate sorts each frame's rows on its own; re-sort across the stack so
    # a dataset's rows stay contiguous and the file is byte-stable run to run.
    sort_by = [c for c in (DATASET_COLUMN, "Cohort", "Barcode", "Visit") if c in ordered.columns]
    return ordered.sort(sort_by) if sort_by else ordered


def main():
    log(f"version {NODE_VERSION}")
    ctx = parse_context()
    config = ctx["config"]
    node = ctx["node"]
    output = ctx["output"]

    cohort = parse_cohort(config.get("cohort"))
    request_id = parse_request_id(config.get("access_request"))
    out_files = output["files"]

    log(f"Node: {node['name']} | Cohort: {cohort or 'all granted'} | "
        f"Request: {request_id or 'auto'}")

    # Optional endpoint override (pass as a pod env var if needed)
    sdk_kwargs = {}
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
            # token_claims decodes the token as a JWT; an opaque API key can't
            # be decoded, and either way the server is what proves it is live.
            try:
                claims = client.token_claims
                who = (claims.get("preferred_username") or claims.get("email")
                       or claims.get("sub") or "unknown")
                log(f"Token loaded; subject: {who}")
            except Exception:
                log("Token loaded (opaque key; claims not decodable)")

            if request_id:
                log(f"Setting access request: {request_id}")
                client.set_access(request_id)

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

                # Phase 1: pull + pivot. aggregate() streams each cohort through
                # an on-disk spool, so this stays memory-bounded; progress=log
                # surfaces its phase timing, which is the only output during a
                # pull that can run for minutes.
                t0 = time.monotonic()
                # source_field picks the wide-column labels; the SDK default is
                # the raw source_field_name, this export wants descriptions.
                frames = aggregate(
                    client,
                    cohort,
                    source_field="source_field_description",
                    progress=log,
                )
                log(f"Aggregate complete in {time.monotonic() - t0:.1f}s; "
                    f"datasets: {sorted(frames)}")

                frame = stack_datasets(frames, index_columns(cohort))
                log(f"Stacked {len(frames)} dataset(s) into one sheet: "
                    f"{frame.height:,} rows x {frame.width:,} cols")

                t_enc = time.monotonic()
                frame.write_parquet(local_path, compression=compression or "uncompressed")
                size_mb = os.path.getsize(local_path) / 1e6
                log(f"Parquet written: {frame.height:,} rows x {frame.width:,} cols, "
                    f"{size_mb:.1f} MB on disk in {time.monotonic() - t_enc:.1f}s "
                    f"(pull+pivot+encode total {time.monotonic() - t0:.1f}s); uploading next")

                # Phase 2: upload the seekable file.
                # Prefers the platform's presigned PUT URL when attached to this
                # output; falls back to boto3 + ARTIFACT_S3_* credentials otherwise
                # (dual mode during the STS-to-presigned migration).
                t1 = time.monotonic()
                main_presigned_url = out_files[0].get("presignedUrl")
                if main_presigned_url:
                    log(f"Uploading {size_mb:.1f} MB -> {out_files[0]['path']} via presigned URL ...")
                    with open(local_path, "rb") as fh:
                        resp = requests.put(main_presigned_url, data=fh, timeout=900)
                    resp.raise_for_status()
                else:
                    # This S3 endpoint doesn't support multipart uploads, so force a
                    # single PUT by raising the threshold above any realistic size.
                    # upload_file streams the body straight from disk, so even a large
                    # single PUT never buffers the whole table in memory.
                    transfer = TransferConfig(
                        multipart_threshold=5 * 1024 * 1024 * 1024,  # 5 GiB: never split
                        use_threads=False,
                    )
                    log(f"Uploading {size_mb:.1f} MB -> s3://{bucket}/{first_key} ...")
                    s3.upload_file(local_path, bucket, first_key, Config=transfer)
                dt = time.monotonic() - t1
                log(f"Upload done in {dt:.1f}s ({size_mb / max(dt, 1e-3):.1f} MB/s)")

                # Additional outputs share the same bytes. There's no presigned
                # equivalent of a server-side copy_object, but the encoded file is
                # still on local disk here, so a presigned extra output gets the
                # same bytes re-PUT directly (no S3 read needed); otherwise fall
                # back to the original single-request server-side copy.
                for f in out_files[1:]:
                    extra_key = s3_key_from_path(f["path"])
                    extra_presigned_url = f.get("presignedUrl")
                    if extra_presigned_url:
                        log(f"Uploading -> {f['path']} via presigned URL (extra output)...")
                        with open(local_path, "rb") as fh:
                            resp = requests.put(extra_presigned_url, data=fh, timeout=900)
                        resp.raise_for_status()
                    else:
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
            "datasets": sorted(frames),
            "cohort": cohort,
            "rows": frame.height,
            "columns": frame.width,
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
