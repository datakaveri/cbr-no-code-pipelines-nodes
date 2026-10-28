# Platform File Source

A source node for the no-code editor. It logs in to the platform **as you**, downloads one
file from a dataset (databank) through the platform's file server, and writes it as Parquet
so the next nodes (SQL, Superset, ...) can use it.

## Use it

1. In the editor, profile menu → Settings → Secrets, create two secrets (uppercase names):
   - `PLATFORM_USERNAME` – your platform login (email)
   - `PLATFORM_PASSWORD` – your platform password
2. Add the node, then fill in:
   - **Dataset (databank) ID** – the id in the dataset's page address.
   - **File path in the dataset** – needed only when the dataset has more than one file.
3. Run. The output is `result.parquet`.

Supported files: CSV, JSON / JSON-lines, GeoJSON, GeoPackage (`.gpkg`) and Parquet. For the map formats each feature becomes
a row and its shape goes into a `geometry` column as GeoJSON text in WGS84 (longitude, latitude); a GeoPackage in another
coordinate system is converted. A GeoPackage with several layers needs the **Layer name** field. Other types give a clear
"not supported yet" error. Files larger than 2 GB are refused (500 MB for one GeoJSON file).

**Many files:** put a pattern (`2026-09-*.parquet`, `data/*.csv`), a folder (`data/`) or `*` (every file) in the file path.
(The **Load all matching files** switch does the same and, with an empty path, loads every file.) The files are stacked into one table (columns matched by name) with an extra
`_source_file` column. **Most files to load** (default 500) stops runaway selections.

## What it does

1. Reads the two secrets (the SDK masks their values in the logs).
2. Password login against the platform's login server (`angular-client`, realm `iudx`),
   which returns a token that lasts about 5 minutes. If the token expires mid-run the node
   logs in once more.
3. `POST /v1/databanks/{id}/files` – lists files; `POST /v1/databanks/{id}/files/download`
   – gets a temporary download link; the file is streamed from that link.
4. CSV / JSON are converted with DuckDB; Parquet is kept as it is; the result is uploaded
   through the editor's storage (the pod holds no storage credentials).

The password and token are never logged or written to the output.

## Config

| Field | Required | Meaning |
|---|---|---|
| `name` | yes | Node name (snake_case) |
| `databank_id` | yes | Dataset id |
| `file_key` | no | File path inside the dataset (a pattern or folder when `all_files` is on) |
| `all_files` | no | Load every matching file into one table |
| `max_files` | no | Safety limit for `all_files` (default 500) |
| `layer` | no | GeoPackage layer name, needed when the file has several |
| `username_secret` / `password_secret` | no | Names of the secrets (defaults above) |
| `file_server_url` / `token_url` | no | Override the platform addresses (for other deployments) |

## Tests

The tests run against a mock login and file server and need no network:

```
cd platform-file-source
docker build -t platform-file-source:test .
docker run --rm -v "$PWD/tests:/app/tests:ro" platform-file-source:test python -m unittest discover -s tests -v
```

## Build on the platform

Images page, new image: repository `https://github.com/pradeep800/pipelines-nodes`, branch `main`,
Dockerfile path `Dockerfile`, Docker context `platform-file-source` (the same values as in `image.json`).
Then Nodes page, add node, paste `node.json` and choose that image.
