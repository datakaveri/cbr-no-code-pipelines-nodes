#!/usr/bin/env python3
"""
SQL Transformer - Custom Node Container

Executes SQL transformations using DuckDB and exports results to S3.
Follows the new NodeContext contract - receives a single NODE_CONTEXT JSON.

Environment Variables:
  NODE_CONTEXT       - JSON object with structure:
                       {
                         "node": { "name": "my_transform", "slug": "sql-transformer" },
                         "inputs": [
                           { "nodeSlug": "csv-source", "nodeName": "orders",
                             "output": { "name": "output", "path": "s3://...", "format": "csv" } }
                         ],
                         "output": {
                           "basePath": "s3://bucket/user/artifacts/flow/exec/nodeName",
                           "files": [{ "name": "result", "format": "parquet",
                                       "path": "s3://artifact-bucket/.../result.parquet" }]
                         },
                         "config": { "sql": "SELECT * FROM ...", ... }
                       }

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

import os
import sys
import json
import tempfile
import duckdb
import requests


def log(message: str):
    print(f"[TRANSFORM] {message}", flush=True)


def log_error(message: str):
    print(f"[TRANSFORM ERROR] {message}", file=sys.stderr, flush=True)


def sanitize_table_name(name: str) -> str:
    """Sanitize a string for use as a SQL table name."""
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)


def parse_node_context() -> dict:
    """Parse NODE_CONTEXT from environment."""
    ctx_str = os.environ.get("NODE_CONTEXT", "")
    if not ctx_str:
        raise ValueError("NODE_CONTEXT environment variable is required")

    try:
        return json.loads(ctx_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse NODE_CONTEXT: {e}")


def create_s3_secret(conn, name: str, prefix: str, bucket_env_var: str):
    """Create a DuckDB S3 secret scoped to one bucket from {prefix}_S3_* env vars.

    Fallback path only, used for an input/output that has no presignedUrl
    (see main()). Two secrets (INPUT + ARTIFACT) are created when their
    credential env vars are present; DuckDB picks the matching one per query
    from the path's bucket, so reads from the read-only upload bucket and
    writes to the read+write artifact bucket each use the right credentials.
    Skipped (not an error) when the env vars are absent — once a future
    backend stops injecting STS credentials, every entry is expected to carry
    a presignedUrl and this fallback is simply unused.
    """
    if not os.environ.get(f"{prefix}_S3_ACCESS_KEY"):
        log(f"No {prefix}_S3_ACCESS_KEY configured, skipping {name} (presigned-URL-only mode)")
        return

    bucket = os.environ[bucket_env_var]
    session_token = os.environ.get(f"{prefix}_S3_SESSION_TOKEN", "")
    session_token_clause = ""
    if session_token:
        session_token_clause = f",\n                SESSION_TOKEN '{session_token}'"

    conn.execute(f"""
        CREATE SECRET {name} (
            TYPE S3,
            KEY_ID '{os.environ[f"{prefix}_S3_ACCESS_KEY"]}',
            SECRET '{os.environ[f"{prefix}_S3_SECRET_KEY"]}',
            ENDPOINT '{os.environ[f"{prefix}_S3_ENDPOINT"]}',
            SCOPE 's3://{bucket}',
            URL_STYLE 'path',
            USE_SSL {os.environ.get(f"{prefix}_S3_USE_SSL", "false").lower()},
            REGION '{os.environ.get(f"{prefix}_S3_REGION", "us-east-1")}'{session_token_clause}
        )
    """)


def validate_context(ctx: dict):
    """Validate required fields in NodeContext."""
    if not ctx.get("node", {}).get("name"):
        raise ValueError("node.name is required in NODE_CONTEXT")

    config = ctx.get("config", {})
    if not config.get("sql"):
        raise ValueError("config.sql is required in NODE_CONTEXT")

    if not ctx.get("inputs"):
        raise ValueError("At least one input is required in NODE_CONTEXT")

    if not ctx.get("output", {}).get("files"):
        raise ValueError("output.files is required in NODE_CONTEXT")


def main():
    # Parse context
    ctx = parse_node_context()

    # Extract context fields
    node = ctx["node"]
    inputs = ctx["inputs"]
    output = ctx["output"]
    config = ctx["config"]

    node_name = node["name"]
    node_slug = node["slug"]
    sql_query = config["sql"]
    output_files = output["files"]

    log("=== SQL Transformer Node ===")
    log(f"Node: {node_name} ({node_slug})")
    log(f"Inputs: {len(inputs)}")
    log(f"Outputs: {len(output_files)}")

    # Log inputs
    for inp in inputs:
        log(f"  Input: {inp['nodeName']} ({inp['nodeSlug']}) -> {inp['output']['format']}")

    validate_context(ctx)

    # Create in-memory DuckDB connection
    conn = duckdb.connect(":memory:")

    try:
        # Load httpfs extension
        log("Loading httpfs extension...")
        conn.load_extension("httpfs")

        # Configure S3 — one scoped secret per bucket. DuckDB selects the
        # matching secret per query from the path's bucket prefix, so inputs
        # from the read-only upload bucket and writes to the read+write
        # artifact bucket both work.
        log("Configuring S3 connection...")
        create_s3_secret(conn, "input_secret", "INPUT", "INPUT_S3_BUCKET")
        create_s3_secret(conn, "artifact_secret", "ARTIFACT", "ARTIFACT_S3_BUCKET")
        log("S3 secrets configured successfully")

        # Load all inputs as tables
        log("Loading inputs...")
        for inp in inputs:
            # Use nodeName as table name (sanitized)
            table_name = sanitize_table_name(inp["nodeName"])
            # A presigned GET URL needs no S3 secret at all — httpfs treats a
            # plain https:// argument as a generic HTTP resource. Falls back
            # to the s3:// path (read via the secrets created above) when the
            # backend hasn't attached one.
            path = inp["output"].get("presignedUrl") or inp["output"]["path"]
            format = inp["output"].get("format", "parquet").lower()

            log(f"  Loading '{table_name}' from {inp['nodeName']} (format: {format})")

            # Determine read function based on format
            if format == "csv":
                read_func = "read_csv_auto"
            elif format == "json":
                read_func = "read_json_auto"
            else:
                read_func = "read_parquet"

            load_sql = f"CREATE TABLE {table_name} AS SELECT * FROM {read_func}('{path}')"
            conn.execute(load_sql)

            result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            count = result[0] if result else 0
            log(f"  Loaded {count} rows into '{table_name}'")

        # Create input_data alias if single input
        if len(inputs) == 1:
            table_name = sanitize_table_name(inputs[0]["nodeName"])
            log(f"Creating 'input_data' alias for '{table_name}'")
            conn.execute(f"CREATE VIEW input_data AS SELECT * FROM {table_name}")

        # Execute SQL transformation
        log("Executing SQL transformation...")
        result_table = sanitize_table_name(node_name)
        create_table_sql = f"CREATE TABLE {result_table} AS {sql_query}"
        conn.execute(create_table_sql)

        result = conn.execute(f"SELECT COUNT(*) FROM {result_table}").fetchone()
        row_count = result[0] if result else 0
        log(f"Transformation produced {row_count} rows")

        # Export results for each output file
        for output_file in output_files:
            output_name = output_file["name"]
            output_format = output_file["format"]
            # Write to the exact path the platform assigned (always artifact bucket).
            output_path = output_file["path"]
            presigned_put_url = output_file.get("presignedUrl")

            log(f"Exporting '{output_name}' to {output_path}...")

            # Get format options
            if output_format == "parquet":
                format_options = "FORMAT PARQUET, COMPRESSION 'snappy'"
            elif output_format == "csv":
                format_options = "FORMAT CSV, HEADER true"
            elif output_format == "json":
                format_options = "FORMAT JSON"
            else:
                format_options = "FORMAT PARQUET"

            if presigned_put_url:
                # Write to a local temp file and PUT it ourselves rather than
                # `COPY ... TO 'https://...'` — DuckDB's HTTP write support
                # varies by version, whereas a local COPY + `requests.put` of
                # a presigned S3 URL works the same everywhere.
                with tempfile.NamedTemporaryFile(suffix=f".{output_format}") as tmp:
                    conn.execute(f"COPY {result_table} TO '{tmp.name}' ({format_options})")
                    with open(tmp.name, "rb") as fh:
                        resp = requests.put(presigned_put_url, data=fh.read(), timeout=600)
                        resp.raise_for_status()
                log(f"  Exported {row_count} rows to {output_path} via presigned URL")
            else:
                export_sql = f"COPY {result_table} TO '{output_path}' ({format_options})"
                conn.execute(export_sql)
                log(f"  Exported {row_count} rows to {output_path}")

        log("=== Transformer Node Complete ===")

        # Output result as JSON
        result_json = {
            "success": True,
            "nodeName": node_name,
            "rowCount": row_count,
            "outputs": [
                {
                    "name": f["name"],
                    "path": f["path"],
                    "format": f["format"],
                }
                for f in output_files
            ],
        }
        print(json.dumps(result_json))

    except Exception as e:
        log_error(f"Failed: {e}")
        result_json = {
            "success": False,
            "nodeName": node_name,
            "error": str(e),
        }
        print(json.dumps(result_json))
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
