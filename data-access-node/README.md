# Data Access Node

A source node for the no-code editor (the "data access node"). It logs in to the platform **as you**,
downloads files from a dataset (databank) through the platform's file server and **keeps the original
files, unchanged, in one folder of your artifacts** (you can see it in the File Browser). One of them
is also written as Parquet, so the next nodes (SQL, Superset, ...) can use it.

## Use it

1. In the editor, profile menu → Settings → Secrets, create two secrets (uppercase names):
   - `PLATFORM_USERNAME` – your platform login (email)
   - `PLATFORM_PASSWORD` – your platform password
2. Add the node, then fill in:
   - **Dataset (databank) ID** – the id in the dataset's page address.
   - **File path in the dataset** – needed only when the dataset has more than one file. A pattern, a folder or `*` downloads several.
   - **File for the next nodes** – which downloaded file the next nodes work on (for example `people.csv`). Leave it empty when only one file is downloaded; it is required when several are.
3. Run. The originals are saved in your artifacts at
   `<pipeline>/<run time>/<node name>/files/<path in the dataset>`, and the output of the node is
   `result.parquet`, made from the file you chose.

Supported files: CSV, JSON / JSON-lines, GeoJSON, GeoPackage (`.gpkg`) and Parquet. For the map formats each feature becomes
a row and its shape goes into a `geometry` column as GeoJSON text in WGS84 (longitude, latitude); a GeoPackage in another
coordinate system is converted. A GeoPackage with several layers needs the **Layer name** field. Other types give a clear
"not supported yet" error. Files larger than 2 GB are refused (500 MB for one GeoJSON file).

**Many files:** put a pattern (`2026-09-*.parquet`, `data/*.csv`), a folder (`data/`) or `*` (every file)
in the file path. Every matching file is saved untouched in the `files` folder, with its folders from the
dataset kept. Files are **not** combined into one table: name the one the next nodes should read in
**File for the next nodes**. The node checks that choice before downloading anything. **Most files to
download** (default 500) stops runaway selections. Files that cannot be read as a table (images, ...) are
still saved; they just cannot be the file for the next nodes. A file name with two dots in a row
(`report..csv`) is stored as `report_csv`, because the storage does not accept `..`.

## What it does

1. Reads the two secrets (the SDK masks their values in the logs).
2. Password login against the platform's login server (`angular-client`, realm `iudx`),
   which returns a token that lasts about 5 minutes. If the token expires mid-run the node
   logs in once more.
3. `POST /v1/databanks/{id}/files` – lists files; `POST /v1/databanks/{id}/files/download`
   – gets a temporary download link; the file is streamed from that link.
4. Each original is uploaded as it is, through the editor's storage (the pod holds no storage
   credentials), into the node's `files` folder.
5. The chosen file is converted with DuckDB (CSV / JSON / GeoJSON / GeoPackage; Parquet is kept as it is)
   and uploaded as `result.parquet`.

The password and token are never logged or written to the output.

## Config

| Field | Required | Meaning |
|---|---|---|
| `name` | yes | Node name (snake_case) |
| `databank_id` | yes | Dataset id |
| `file_key` | no | File path inside the dataset, or a pattern / folder / `*` to download several files |
| `use_file` | when several files | The downloaded file the next nodes read (full path, or just the name if unambiguous) |
| `max_files` | no | Most files a pattern may download (default 500) |
| `layer` | no | GeoPackage layer name, needed when the file has several |
| `username_secret` / `password_secret` | no | Names of the secrets (defaults above) |
| `file_server_url` / `token_url` | no | Override the platform addresses (for other deployments) |

## Tests

The tests run against a mock login and file server and need no network:

```
cd data-access-node
docker build -t data-access-node:test .
docker run --rm -v "$PWD/tests:/app/tests:ro" data-access-node:test python -m unittest discover -s tests -v
```

## Build on the platform

Images page, new image: repository `https://github.com/pradeep800/pipelines-nodes`, branch `main`,
Dockerfile path `Dockerfile`, Docker context `data-access-node` (the same values as in `image.json`).
Then Nodes page, add node, paste `node.json` and choose that image.
