# cbr-data-access

Python SDK for authenticated data access to the CBR / datakaveri platform.

It wraps a three-step flow behind a single client:

1. **Read** the platform-provided bearer token from the notebook sidecar.
2. **Fetch** managed PostgreSQL connection metadata from the connection-info
   service using that token.
3. **Connect** to PostgreSQL with SQLAlchemy/psycopg2 or Apache Arrow ADBC — the
   access token is used as the database password — and run read queries
   (returned as pandas DataFrames by default, or polars via
   `dataframe="polars"`).

Data access is granted per **access request**: an approved request unlocks a set
of cohorts and concepts, and each of its tables is queryable through the proxy as
a `req_<request_id>.<table>` relation. Behind each relation the server constructs
a `UNION ALL` across the request's cohorts, row-filtered to the granted concept
ids, with a `cohort_id` column telling you which cohort each row came from.

> For a full walkthrough of every feature with copy-paste examples, see the
> [Usage Guide](USAGE.md).

## Requirements

- Python >= 3.10
- Network access to the connection-info service.
- A platform token at `/var/run/sandbox-connect/platform/token` (or set
  `CBR_TOKEN_FILE` to another token file).

## Installation

```bash
pip install cbr-data-access
```

For local development (editable install with dev tooling):

```bash
pip install -e ".[dev]"
```

## Quickstart

```python
from cbr_data_access import DataAccessClient

with DataAccessClient() as client:
    df = client.query("SELECT * FROM person LIMIT 10")
    print(df)
```

Use the Arrow-native ADBC transport through the same client:

```python
with DataAccessClient(driver="adbc") as client:
    df = client.query("SELECT * FROM person LIMIT 10")
    arrow_table = client.query_arrow("SELECT * FROM person LIMIT 10")
```

ADBC currently supports non-parameterized reads. Keep using the default
SQLAlchemy driver when bind parameters are required.

### Opaque PostgreSQL types with ADBC

The ADBC PostgreSQL driver represents database types it cannot interpret as
Arrow `arrow.opaque` extension columns. When an ADBC query returns a Polars
DataFrame, the SDK loads those columns as their declared Arrow storage type.
This preserves the values, avoids Polars' unknown-extension warning, and keeps
the behavior stable when Polars 2.0 changes its default extension handling.

Use `query_arrow()` or `query_arrow_stream()` when you need native Arrow
results: those methods retain the original `arrow.opaque` annotation and its
PostgreSQL type metadata. The SDK does not attempt to assign vendor-specific
semantics to opaque values.

Normal use is just `client.query(...)` — it authenticates, connects,
disconnects, and resolves plain table names against your approved access
request for you. You don't call `connect()` yourself. If you have **several**
approved requests, pick one once with `client.set_access(request_id)`; with
exactly one, nothing else is needed.

> Don't commit real tokens to source.

## Discovering your access requests

List your access requests and what each approved one makes queryable:

```python
with DataAccessClient() as client:
    requests = client.list_requests()

    print(requests.to_frame())     # one row per (request, table) — id, status, relation
    print(requests.ids)            # ['dc43d8d9-6f6c-4152-b5f1-0f9668ba01e4', ...]

    request = requests.approved[0]
    print(request.table_names)     # ['measurement', 'person', ...]
    print(request.concepts_frame())  # the concept grants behind the request
```

Pending/rejected requests are listed too, but expose no tables. If you have
exactly one approved request you can omit `request_id` everywhere; with several,
pass it explicitly (the UUID or the `req_...` form both work).

## Row counts

Use `client.row_counts()` (or `client.row_counts(tables=["person"])`) to get a
`{table_name: row_count}` dict — a cheap `COUNT(*)` per table, handy for sizing
up a table before pulling it:

```python
with DataAccessClient() as client:
    print(client.row_counts())   # {'measurement': 12345, 'person': 110}
```

## Configuration

The client reads the platform token file by default. Endpoint settings only need
to be set if you target a different deployment.

| Constructor argument   | Required | Default |
|------------------------|----------|---------|
| `token_file`           | no       | `CBR_TOKEN_FILE` or `/var/run/sandbox-connect/platform/token` |
| `base_url`             | no       | `http://127.0.0.1:6060` |
| `dataframe`            | no       | `"pandas"` — what `query()`/`query_stream()` return; `"polars"` to switch (either call can override per invocation) |
| `driver`               | no       | `"sqlalchemy"`; use `"adbc"` for Arrow-native PostgreSQL reads |

`base_url` is the root of the data-access service; the connection-info
(`/postgres/connection-info`) and requests (`/requests`) endpoints hang off it.

## API overview

- **`DataAccessClient(*, token_file=..., base_url=..., request_timeout=300.0, dataframe="pandas", driver="sqlalchemy")`**
  — construct a client (no network I/O until first use). Usable as a context
  manager; exiting disposes the connection pool (`client.close()` does the same,
  and the client stays usable — it reconnects lazily on the next call).
- **`client.login() -> str`** — read or obtain and cache the token (called
  lazily when needed; an expired platform token is read from the token file
  again).
- **`client.token_claims -> dict`** — decoded claims of the current token.
- **`client.set_access(request_id=None) -> AccessRequest`** — pick which
  approved request to query (only needed when you have several; with one it is
  resolved automatically). `client.default_request` shows the current pick;
  `client.clear_access()` forgets it.
- **`client.query(sql, params=None, *, dataframe=None) -> DataFrame`** — the
  normal way to read data; no `connect()` call required, as `query()` connects
  and disconnects for you. Returns pandas by default, polars with
  `dataframe="polars"`. Prefer bind parameters:
  `client.query("... WHERE id = :id", {"id": 1})`.
- **`client.query_stream(sql, params=None, *, chunksize=50_000, dataframe=None) -> Iterator[DataFrame]`**
  — like `query()`, but yields the result in DataFrame batches (pandas by
  default, polars with `dataframe="polars"`), so peak DataFrame memory stays
  bounded for arbitrarily large results. The connection is released when the
  iterator is exhausted or closed.
- **`client.connect()`** — context manager yielding a live SQLAlchemy
  `Connection`, closed on exit. Use it as `with client.connect() as conn:`;
  calling it bare does nothing. For power users who want raw SQLAlchemy access —
  you don't need it for normal queries.
- **`client.connect_adbc()`** — context manager yielding a native ADBC DBAPI
  connection.
- **`client.query_arrow(sql) -> pyarrow.Table`** — execute through ADBC and
  return the native Arrow table, regardless of the client's default driver.
- **`client.query_arrow_stream(sql) -> Iterator[pyarrow.RecordBatch]`** — stream
  native Arrow record batches through ADBC.
- **`client.list_requests() -> RequestList`** — list your access requests.
  Returns a `RequestList` (a sequence of `AccessRequest`, each carrying its
  queryable `tables` and the `concepts` granted). Call `.to_frame()` on the
  result for a pandas DataFrame you can browse and filter.
- **`client.row_counts(*, request_id=None, tables=None) -> dict[str, int]`** —
  return a `{table_name: row_count}` dict (a cheap `COUNT(*)` per table); handy
  for sizing up a table before pulling it.
- **`client.aggregate(cohort=None, *, gender_male=8507, gender_female=8532, source_field="source_field_name", max_workers=1, progress=False) -> dict[str, polars.DataFrame]`**
  — wide-format OMOP aggregate of your granted data, one frame per dataset.
  Set `source_field="source_field_description"` to prefer descriptions as
  column headers; missing or empty values fall back to the other source field.
  With `driver="adbc"`, the long-format pull streams native Arrow batches
  directly into its Parquet spool; the default driver retains the existing
  SQLAlchemy/psycopg2 path.
  Always returns **polars** DataFrames, regardless of the `dataframe` setting
  (use `.to_pandas()` on a frame if you need pandas).
- **`decode_token(token) -> dict`** — decode a JWT's claims without verifying
  its signature.

### Exceptions

All SDK errors derive from `DataAccessError` (which carries an optional
`.status_code`):

- `AuthenticationError` — platform token loading failed or a service rejected the token.
- `ConnectionInfoError` — connection-info endpoint failed or returned nothing.
- `RequestsError` — the requests endpoint failed, a referenced request/table is
  not in your catalog, or a request is not approved.
- `DBConnectionError` — establishing the database connection failed.
- `QueryError` — query execution failed.

## Authentication & security notes

- The access token is **short-lived** and is used as the database password; the
  client rereads the platform token file when the cached token nears expiry.
- Access is request-scoped server-side: only relations granted by an APPROVED
  access request resolve, queries are read-only, and concept-level row filters
  are applied inside the server-constructed queries — they cannot be bypassed
  from SQL.
- Rotate any credentials or tokens that have ever been committed to source.

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

ruff check . && ruff format --check .
mypy
pytest
```

## License

Apache-2.0. See [LICENSE](LICENSE).
