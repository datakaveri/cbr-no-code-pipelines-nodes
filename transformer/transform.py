#!/usr/bin/env python3
"""
SQL Transformer - Custom Node Container

Executes SQL transformations using DuckDB and exports results to S3.
Follows the new NodeContext contract - receives a single NODE_CONTEXT JSON.
All storage reads/writes go through storage_v2 (presigned URLs via the
backend's API-key-authenticated API) — see storage_v2.py for its required
env vars (BACKEND_URL, API_KEY, NODE_CONTEXT).

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
"""

import os
import sys
import json
import tempfile
import duckdb
import requests
from storage_v2 import open_write, read_to_file


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
        # Load all inputs as tables
        log("Loading inputs...")
        for inp in inputs:
            # Use nodeName as table name (sanitized)
            table_name = sanitize_table_name(inp["nodeName"])
            suffix = "." + inp["output"].get("format", "parquet")
            local_input = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
            local_input.close()
            read_to_file(inp["output"]["path"], local_input.name)
            path = local_input.name
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

            with tempfile.NamedTemporaryFile(suffix=f".{output_format}") as tmp:
                conn.execute(f"COPY {result_table} TO '{tmp.name}' ({format_options})")
                with open(tmp.name, "rb") as source, open_write(output_path) as destination:
                    while block := source.read(1024 * 1024):
                        destination.write(block)
            log(f"  Exported {row_count} rows to {output_path} via Storage v2")

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
