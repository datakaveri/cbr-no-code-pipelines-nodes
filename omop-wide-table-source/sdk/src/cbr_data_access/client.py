"""Client for the CBR / datakaveri data-access platform.

Flow:
    1. Read the platform bearer token from the sidecar-managed token file
       (``CBR_TOKEN_FILE`` or the default platform path).
    2. Call the connection-info endpoint with that token -> DB connection
       metadata (host, port, database, username).
    3. Connect to PostgreSQL via SQLAlchemy/psycopg2 or Apache Arrow ADBC,
       using the access token as the database password.
    4. Run SQL with plain table names against your **approved access request**
       — the client resolves them against it automatically (``set_access()``
       picks one when you have several approved requests).

Example:
    >>> from cbr_data_access import DataAccessClient
    >>> with DataAccessClient() as client:
    ...     df = client.query("SELECT count(*) FROM person")
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal, overload
from urllib.parse import quote

import jwt
import pandas as pd
import polars as pl
import requests
from sqlalchemy import URL, Connection, Engine, create_engine, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import SQLAlchemyError

from . import config
from ._arrow import opaque_to_storage
from ._sql import qualify_statement
from .access_requests import AccessRequest, RequestList, RequestTable
from .exceptions import (
    ConnectionInfoError,
    DataAccessError,
    DBConnectionError,
    QueryError,
    RequestsError,
)
from .platform_auth import read_platform_token

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger(__name__)

_EXP_SKEW_SECONDS = 30  # treat the token as expired this many seconds early

# Quotes SQL identifiers (and doubles embedded quotes) for PostgreSQL. Schema and
# table names are identifiers, not values, so they can't be bind parameters —
# they must be safely quoted when interpolated into a query.
_IDENT = postgresql.dialect().identifier_preparer

# Rows pulled per batch when streaming query results. Bounds peak memory: only
# this many rows are held at once regardless of how large the result set is.
_CHUNK_SIZE = 50_000

# DataFrame libraries query() / query_stream() can return results as.
_DATAFRAME_LIBS = ("pandas", "polars")

# SQLAlchemy/psycopg2 remains the backwards-compatible default; ADBC is a
# second transport exposed by the same client and package.
_DATABASE_DRIVERS = ("sqlalchemy", "adbc")


def _check_dataframe(dataframe: str) -> str:
    """Validate a dataframe-library choice, returning it unchanged.

    Raises:
        DataAccessError: If ``dataframe`` is neither ``"pandas"`` nor ``"polars"``.
    """
    if dataframe not in _DATAFRAME_LIBS:
        raise DataAccessError(f"Unsupported dataframe: {dataframe!r} (use 'pandas' or 'polars')")
    return dataframe


def _check_driver(driver: str) -> str:
    """Validate a database-driver choice, returning it unchanged."""
    if driver not in _DATABASE_DRIVERS:
        raise DataAccessError(f"Unsupported driver: {driver!r} (use 'sqlalchemy' or 'adbc')")
    return driver


def _adbc_postgresql_uri(info: dict[str, Any], token: str) -> str:
    """Build a libpq URI without leaking or incorrectly escaping the token."""
    username = quote(str(info.get("username") or ""), safe="")
    password = quote(token, safe="")
    database = quote(str(info.get("database") or ""), safe="")
    host = str(info.get("host") or "")
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = str(info.get("port") or "")
    authority = f"{host}:{port}" if port else host
    return f"postgresql://{username}:{password}@{authority}/{database}?sslmode=disable"


def _load_adbc_dbapi() -> Any:
    """Import the bundled ADBC driver with an actionable installation error."""
    try:
        import adbc_driver_postgresql.dbapi as adbc
    except ImportError as exc:
        raise DBConnectionError(
            "ADBC support is unavailable; reinstall cbr-data-access with its dependencies"
        ) from exc
    return adbc


def _select_tables(request: AccessRequest, tables: Iterable[str] | None) -> list[RequestTable]:
    """Resolve requested table names against a request (all tables if ``None``).

    Raises:
        RequestsError: If a requested name is not granted by the request.
    """
    if tables is None:
        return list(request.tables)
    selected: list[RequestTable] = []
    for name in tables:
        table = request.get_table(name)
        if table is None:
            raise RequestsError(f"Table {name!r} is not granted by request {request.id}")
        selected.append(table)
    return selected


def decode_token(token: str) -> dict[str, Any]:
    """Decode a JWT's claims WITHOUT verifying its signature.

    Useful for introspection and for reading ``exp``. This does not validate
    the token's signature, issuer, or audience.
    """
    return jwt.decode(token, options={"verify_signature": False})


class DataAccessClient:
    """Encapsulates auth, access-request retrieval, and database access.

    Authentication is lazy: constructing the client performs no network I/O.
    The access token and SQLAlchemy engine are cached on the instance and
    reused; an expired token triggers automatic re-authentication.

    Usage:
        :meth:`query` is the primary entry point and handles everything for you
        (auth, connecting, disconnecting, and resolving plain table names
        against your approved access request) — you do not need to call
        :meth:`connect`. With several approved requests, pick one first via
        :meth:`set_access`. :meth:`list_requests` lists your access requests
        and what each one grants. :meth:`row_counts` reports how many rows each
        table has. :meth:`connect` is an optional context manager for power
        users who want a raw SQLAlchemy ``Connection``.

    Args:
        token_file: Platform token file. Defaults to ``CBR_TOKEN_FILE`` or the
            sidecar-managed platform token path.
        base_url: Base URL of the data-access service; the connection-info and
            requests endpoints hang off it.
        request_timeout: Per-request HTTP timeout, in seconds (default 300, i.e.
            5 minutes).
        dataframe: Default DataFrame library that :meth:`query` and
            :meth:`query_stream` return results as — ``"pandas"`` (the default)
            or ``"polars"``. Either call can override it per-invocation with its
            own ``dataframe`` argument.
        driver: Database transport used by :meth:`query` and
            :meth:`query_stream`: ``"sqlalchemy"`` (the backwards-compatible
            default) or Arrow-native ``"adbc"``. Raw :meth:`connect` remains a
            SQLAlchemy connection; use :meth:`connect_adbc` for raw ADBC access.

    Raises:
        DataAccessError: If ``dataframe`` or ``driver`` is unsupported.

    Example:
        >>> with DataAccessClient() as client:
        ...     print(client.list_requests().to_frame())
        ...     df = client.query("SELECT * FROM person LIMIT 10")
    """

    def __init__(
        self,
        *,
        token_file: str | os.PathLike[str] | None = None,
        base_url: str = config.DEFAULT_BASE_URL,
        request_timeout: float = 300.0,
        dataframe: str = "pandas",
        driver: str = "sqlalchemy",
    ) -> None:
        self._token_file = token_file
        self._base_url = base_url.rstrip("/")
        self._connection_info_url = f"{self._base_url}{config.CONNECTION_INFO_PATH}"
        self._requests_url = f"{self._base_url}{config.REQUESTS_PATH}"
        self._timeout = request_timeout
        self._dataframe = _check_dataframe(dataframe)
        self._driver = _check_driver(driver)

        self._platform_token: str | None = None
        self._token_exp: float | None = None
        self._connection_info: dict[str, Any] | None = None
        self._engine: Engine | None = None
        self._engine_token: str | None = None  # token the cached engine was built with
        self._default_request: AccessRequest | None = None  # pinned by set_access()

    # ------------------------------------------------------------------ #
    # context manager
    # ------------------------------------------------------------------ #

    def __enter__(self) -> DataAccessClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Dispose the cached SQLAlchemy engine and its connection pool."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self._engine_token = None

    # ------------------------------------------------------------------ #
    # authentication
    # ------------------------------------------------------------------ #

    def login(self) -> str:
        """Load or obtain a bearer token and cache it.

        Returns:
            The access token.

        Raises:
            AuthenticationError: If the platform token is unavailable.
        """
        token = read_platform_token(self._token_file)
        self._cache_platform_token(token)
        logger.debug("Loaded platform token; exp=%s", self._token_exp)
        return token

    def _cache_platform_token(self, token: str) -> None:
        if self._platform_token != token:
            self.close()
            self._connection_info = None
        self._platform_token = token
        self._token_exp = self._read_exp(token)

    @staticmethod
    def _read_exp(token: str) -> float | None:
        """Read the ``exp`` claim, returning None if absent or unparseable."""
        try:
            exp = decode_token(token).get("exp")
            return float(exp) if exp is not None else None
        except Exception:
            return None

    def _token_expired(self) -> bool:
        if self._token_exp is None:
            return False
        return time.time() >= (self._token_exp - _EXP_SKEW_SECONDS)

    def _ensure_token(self) -> str:
        """Return a valid token, (re)authenticating if missing or expired."""
        if self._platform_token is None or self._token_expired():
            self.login()
        assert self._platform_token is not None  # narrowed for type-checkers
        return self._platform_token

    @property
    def token_claims(self) -> dict[str, Any]:
        """Decoded claims of the current token (authenticates if needed)."""
        return decode_token(self._ensure_token())

    # ------------------------------------------------------------------ #
    # connection info
    # ------------------------------------------------------------------ #

    def _fetch_connection_info(self) -> dict[str, Any]:
        token = self._ensure_token()
        headers = {"Authorization": f"Bearer {token}"}
        try:
            response = requests.get(
                self._connection_info_url, headers=headers, timeout=self._timeout
            )
        except requests.RequestException as exc:
            raise ConnectionInfoError(f"Connection-info request failed: {exc}") from exc

        if response.status_code != 200:
            raise ConnectionInfoError(response.text, status_code=response.status_code)

        info = response.json()
        if not info:
            raise ConnectionInfoError("Empty connection-info response")
        return info

    def _ensure_connection_info(self) -> dict[str, Any]:
        if self._connection_info is None:
            self._connection_info = self._fetch_connection_info()
        return self._connection_info

    # ------------------------------------------------------------------ #
    # access requests / catalog
    # ------------------------------------------------------------------ #

    def list_requests(self) -> RequestList:
        """List the caller's access requests and what each one makes queryable.

        Calls the requests endpoint with the current access token and returns
        the catalog as structured objects. The result is a sequence of
        :class:`~cbr_data_access.access_requests.AccessRequest` (iterate or
        index it) that also carries ``username``. Approved requests expose
        their queryable tables (each with its ready-to-use
        ``req_<request_id>.<table>`` relation); pending/rejected requests are
        listed for visibility but expose none.

        Returns:
            A :class:`~cbr_data_access.access_requests.RequestList`.

        Raises:
            AuthenticationError: If authentication fails.
            RequestsError: On transport failure or a non-200 response.

        Example:
            >>> reqs = client.list_requests()
            >>> [r.id for r in reqs.approved]
            ['dc43d8d9-6f6c-4152-b5f1-0f9668ba01e4']
            >>> reqs.to_frame()  # one row per (request, table), handy for browsing
        """
        token = self._ensure_token()
        headers = {"Authorization": f"Bearer {token}"}
        try:
            response = requests.get(self._requests_url, headers=headers, timeout=self._timeout)
        except requests.RequestException as exc:
            raise RequestsError(f"Requests catalog request failed: {exc}") from exc

        if response.status_code != 200:
            raise RequestsError(response.text, status_code=response.status_code)

        return RequestList.from_dict(response.json())

    def _resolve_request(self, catalog: RequestList, request_id: str | None) -> AccessRequest:
        """Resolve ``request_id`` (or the sole approved request) in the catalog.

        Raises:
            RequestsError: If the id is unknown, the request is not approved, or
                ``request_id`` is None while zero / more than one request is
                approved.
        """
        if request_id is not None:
            request = catalog.get(request_id)
            if request is None:
                raise RequestsError(f"Unknown access request: {request_id!r}")
            if not request.is_approved:
                raise RequestsError(
                    f"Access request {request.id} is not approved (status: {request.status!r})"
                )
            return request

        approved = catalog.approved
        if len(approved) == 1:
            return approved[0]
        if not approved:
            raise RequestsError("You have no approved access requests")
        ids = ", ".join(r.id for r in approved)
        raise RequestsError(
            f"You have {len(approved)} approved access requests — pass request_id "
            f"to pick one of: {ids}"
        )

    def set_access(self, request_id: str | None = None) -> AccessRequest:
        """Pin an approved access request as this client's default.

        You only need this when you have **several** approved requests — with
        exactly one, every method resolves it automatically. After pinning,
        bare table names in :meth:`query` and :meth:`query_stream` resolve
        against the pinned request's schema, and :meth:`row_counts` defaults
        to it:

            >>> client.set_access("dc43d8d9-6f6c-4152-b5f1-0f9668ba01e4")
            >>> client.query("SELECT * FROM person LIMIT 5")

        Args:
            request_id: The access request to pin — the UUID or the ``req_...``
                schema form. ``None`` (the default) pins your sole approved
                request and raises if you have zero or several.

        Returns:
            The pinned :class:`~cbr_data_access.access_requests.AccessRequest`.

        Raises:
            RequestsError: If listing fails, the id is unknown, the request is
                not approved, or ``request_id`` is None while zero / more than
                one request is approved.
        """
        request = self._resolve_request(self.list_requests(), request_id)
        self._default_request = request
        return request

    def clear_access(self) -> None:
        """Forget the pinned request; the next query re-resolves automatically."""
        self._default_request = None

    @property
    def default_request(self) -> AccessRequest | None:
        """The pinned access request, or ``None`` if none is pinned yet."""
        return self._default_request

    def _ensure_access(self) -> AccessRequest:
        """The pinned request, auto-pinning the sole approved one if needed.

        Raises:
            RequestsError: If listing fails, or zero / more than one request is
                approved (call :meth:`set_access` to pick one).
        """
        if self._default_request is None:
            self._default_request = self._resolve_request(self.list_requests(), None)
        return self._default_request

    def _effective_request_id(self, request_id: str | None) -> str | None:
        """An explicit ``request_id``, else the pinned default's id, else None."""
        if request_id is not None:
            return request_id
        if self._default_request is not None:
            return self._default_request.id
        return None

    # ------------------------------------------------------------------ #
    # dataframe library
    # ------------------------------------------------------------------ #

    @property
    def dataframe(self) -> str:
        """The default DataFrame library for results (``"pandas"`` or ``"polars"``)."""
        return self._dataframe

    @property
    def driver(self) -> str:
        """The query driver: ``"sqlalchemy"`` (default) or ``"adbc"``."""
        return self._driver

    def _resolve_dataframe(self, dataframe: str | None) -> str:
        """An explicit per-call choice if given, else the client default.

        Raises:
            DataAccessError: If an explicit choice is not ``"pandas"`` or ``"polars"``.
        """
        if dataframe is None:
            return self._dataframe
        return _check_dataframe(dataframe)

    # ------------------------------------------------------------------ #
    # row counts
    # ------------------------------------------------------------------ #

    def row_counts(
        self,
        *,
        request_id: str | None = None,
        tables: Iterable[str] | None = None,
    ) -> dict[str, int]:
        """Return the number of rows in each of a request's tables.

        Handy for sizing up a table before querying it — you can see which
        tables are large and decide whether to add a ``LIMIT`` or stream with
        :meth:`query_stream`. Runs a cheap ``COUNT(*)`` per table (no row data
        is transferred), so it is safe even for very large tables.

        Args:
            request_id: Which access request to count (``None`` = your sole
                approved request).
            tables: Optional subset of table names to count. ``None`` (the
                default) counts every granted table.

        Returns:
            A dict mapping each table's name to its row count, in catalog order.

        Raises:
            RequestsError: If listing fails, the request can't be resolved, or a
                requested table name isn't granted by it.
            QueryError: If a count query fails.

        Example:
            >>> client.row_counts()
            {'measurement': 12345, 'person': 110}
        """
        request = self._resolve_request(
            self.list_requests(), self._effective_request_id(request_id)
        )
        selected = _select_tables(request, tables)
        counts: dict[str, int] = {}
        for table in selected:
            sql = f"SELECT count(*) AS n FROM {self._quote_relation(request, table)}"  # noqa: S608
            df = self.query(sql, dataframe="pandas")
            counts[table.name] = int(df.iloc[0, 0])
        return counts

    @staticmethod
    def _quote_relation(request: AccessRequest, table: RequestTable) -> str:
        """Render a request table as a safely quoted ``schema.table`` SQL reference."""
        return f"{_IDENT.quote(request.schema)}.{_IDENT.quote(table.name)}"

    # ------------------------------------------------------------------ #
    # aggregation / export (see aggregate.py)
    # ------------------------------------------------------------------ #

    def aggregate(
        self,
        cohort: int | str | None = None,
        *,
        gender_male: int = 8507,
        gender_female: int = 8532,
        source_field: Literal["source_field_name", "source_field_description"] = (
            "source_field_name"
        ),
        max_workers: int = 1,
        progress: bool | Callable[[str], None] = False,
    ) -> dict[str, pl.DataFrame]:
        """Wide-format aggregate of your granted OMOP data, one frame per dataset.

        Runs the long-format pull through this client (the proxy), one query per
        cohort, then pivots each ``local_concept.dataset`` to wide format. The
        selected driver is honored: ADBC streams native Arrow batches into the
        Parquet spool, while SQLAlchemy uses its forward-only cursor path. See
        :func:`cbr_data_access.aggregate.aggregate`.

        Args:
            cohort: A single cohort to export, by id (``101``) or by its name
                in the request's ``cohort_mappings`` table (e.g. ``"SANSCOG"``,
                ``"TLSA"``); drops the ``Cohort`` column. ``None`` exports every
                granted cohort that is active in ``cohort_mappings`` with a
                ``Cohort`` column carrying the cohort names.
            gender_male / gender_female: Gender concept ids used to label rows.
            source_field: Local-concept column used first for wide-sheet headers.
                Defaults to ``"source_field_name"``; choose
                ``"source_field_description"`` to prefer descriptions. Missing
                or empty values fall back to the other column.
            max_workers: Max cohorts fetched concurrently (capped at the cohort
                count). Defaults to 1; pass a higher value to opt into parallel
                cohort pulls.
            progress: If true, print aggregate phase timing. If callable, it
                receives each progress message.
        """
        from .aggregate import aggregate as _aggregate

        return _aggregate(
            self,
            cohort,
            gender_male=gender_male,
            gender_female=gender_female,
            source_field=source_field,
            max_workers=max_workers,
            progress=progress,
        )

    # ------------------------------------------------------------------ #
    # engine / connection
    # ------------------------------------------------------------------ #

    def _get_engine(self) -> Engine:
        """Return a cached engine, rebuilding it when the token has refreshed.

        The access token doubles as the database password, so a refreshed token
        invalidates the cached engine's URL; the stale engine is disposed and a
        new one is created.
        """
        token = self._ensure_token()
        if self._engine is not None and self._engine_token == token:
            return self._engine

        info = self._ensure_connection_info()
        if self._engine is not None:
            self._engine.dispose()

        url = URL.create(
            drivername="postgresql+psycopg2",
            username=info.get("username"),
            password=token,
            host=info.get("host"),
            port=info.get("port"),
            database=info.get("database"),
        )
        try:
            self._engine = create_engine(
                url,
                # Long aggregate pulls sit on one connection for minutes; if the
                # proxy/NAT path silently drops it, a bare socket read blocks
                # forever and the client looks hung. Bound the TCP connect and
                # let keepalives surface a dead peer as an error instead.
                connect_args={
                    "connect_timeout": 10,
                    "keepalives": 1,
                    "keepalives_idle": 30,
                    "keepalives_interval": 10,
                    "keepalives_count": 5,
                },
            )
        except SQLAlchemyError as exc:
            raise DBConnectionError(f"Failed to create engine: {exc}") from exc
        self._engine_token = token
        return self._engine

    @contextmanager
    def connect(self) -> Iterator[Connection]:
        """Yield a live SQLAlchemy :class:`Connection`, closed on exit.

        For normal queries you do **not** need this — just call :meth:`query`.
        This is for power users who want a raw SQLAlchemy ``Connection`` (e.g. to
        run several statements or use SQLAlchemy Core directly).

        This is a context manager: it only does anything inside a ``with`` block.
        Calling ``client.connect()`` on its own does nothing and connects to
        nothing — it just returns an un-entered context manager. Always use it
        as ``with client.connect() as conn:``.

        Raises:
            DBConnectionError: If a connection cannot be established (raised when
                the ``with`` block is entered).

        Example:
            >>> with client.connect() as conn:
            ...     conn.execute(text("SELECT 1"))
        """
        engine = self._get_engine()
        try:
            conn = engine.connect()
        except SQLAlchemyError as exc:
            raise DBConnectionError(f"Failed to connect: {exc}") from exc
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def connect_adbc(self) -> Iterator[Any]:
        """Yield a native Apache Arrow ADBC PostgreSQL connection.

        A fresh connection is used for each context so concurrent SDK calls do
        not share an ADBC connection. The bearer token is percent-encoded into
        the libpq URI as the password and is never logged.
        """
        adbc = _load_adbc_dbapi()
        uri = _adbc_postgresql_uri(self._ensure_connection_info(), self._ensure_token())
        try:
            conn = adbc.connect(uri=uri, autocommit=True)
        except adbc.Error as exc:
            raise DBConnectionError(f"Failed to connect with ADBC: {exc}") from exc
        try:
            yield conn
        finally:
            conn.close()

    def _execute_adbc_arrow(self, sql: str) -> pa.Table:
        """Execute already-qualified SQL and materialize its Arrow table."""
        adbc = _load_adbc_dbapi()
        try:
            with self.connect_adbc() as conn, conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetch_arrow_table()
        except DBConnectionError:
            raise
        except adbc.Error as exc:
            raise QueryError(f"ADBC query failed: {exc}") from exc

    def query_arrow(self, sql: str) -> pa.Table:
        """Run a read query through ADBC and return a native PyArrow table.

        The server's initial ADBC compatibility path is intentionally
        non-parameterized. Use literal-free SQL or the SQLAlchemy query path
        when bind parameters are required.
        """
        sql = qualify_statement(sql, self._ensure_access().schema)
        return self._execute_adbc_arrow(sql)

    def query_arrow_stream(self, sql: str) -> Iterator[pa.RecordBatch]:
        """Run a read query through ADBC and yield native Arrow record batches."""
        sql = qualify_statement(sql, self._ensure_access().schema)

        def _stream() -> Iterator[pa.RecordBatch]:
            adbc = _load_adbc_dbapi()
            try:
                with self.connect_adbc() as conn, conn.cursor() as cursor:
                    cursor.execute(sql)
                    yield from cursor.fetch_record_batch()
            except DBConnectionError:
                raise
            except adbc.Error as exc:
                raise QueryError(f"ADBC query failed: {exc}") from exc

        return _stream()

    # ------------------------------------------------------------------ #
    # query convenience
    # ------------------------------------------------------------------ #

    @overload
    def query(
        self, sql: str, params: dict[str, Any] | None = ..., *, dataframe: Literal["pandas"]
    ) -> pd.DataFrame: ...
    @overload
    def query(
        self, sql: str, params: dict[str, Any] | None = ..., *, dataframe: Literal["polars"]
    ) -> pl.DataFrame: ...
    @overload
    def query(
        self, sql: str, params: dict[str, Any] | None = ..., *, dataframe: str | None = ...
    ) -> pd.DataFrame | pl.DataFrame: ...

    def query(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        dataframe: str | None = None,
    ) -> pd.DataFrame | pl.DataFrame:
        """Run a read query and return the result as a DataFrame.

        This is the normal way to use the client; you do **not** need to call
        :meth:`connect` first. Each call authenticates if needed, opens its own
        connection, and releases it when done.

        Write plain table names — they resolve against your approved access
        request automatically (with several approved requests, pick one first
        via :meth:`set_access`):

            >>> client.query("SELECT * FROM person WHERE person_id = :pid", {"pid": 42})

        Prefer bind parameters over string interpolation for values when using
        the default SQLAlchemy driver. The initial ADBC proxy path accepts only
        non-parameterized reads.

        Args:
            sql: The SELECT to run, with plain table names.
            params: Optional bind parameters.
            dataframe: Which DataFrame library to return — ``"pandas"`` or
                ``"polars"``. ``None`` (the default) uses the client's default
                (set at construction; ``"pandas"`` unless changed).

        Returns:
            A ``pandas.DataFrame`` or ``polars.DataFrame``, per the resolved
            ``dataframe`` choice.

        Raises:
            DataAccessError: If ``dataframe`` is not ``"pandas"`` or ``"polars"``.
            RequestsError: If your approved request can't be resolved.
            QueryError: If executing the query fails.
        """
        engine = self._resolve_dataframe(dataframe)
        sql = qualify_statement(sql, self._ensure_access().schema)
        if self._driver == "adbc":
            if params:
                raise QueryError(
                    "ADBC queries do not yet support bind parameters through this proxy"
                )
            table = self._execute_adbc_arrow(sql)
            if engine == "polars":
                return pl.from_arrow(opaque_to_storage(table))
            return table.to_pandas()

        statement: Any = text(sql) if params else sql
        try:
            with self.connect() as conn:
                if engine == "polars":
                    exec_opts = {"parameters": params} if params else None
                    # Fetch in batches on a plain forward-only cursor (NOT
                    # stream_results — the proxy rejects server-side cursors with
                    # "cursor can only scan forward, 55000"). iter_batches just
                    # loops result.fetchmany(batch_size); polars turns each batch
                    # straight into Arrow and drops the Python rows, so peak
                    # memory is one batch instead of the entire result set
                    # materialized as Row objects via a single fetchall().
                    batches = list(
                        pl.read_database(
                            statement,
                            connection=conn,
                            iter_batches=True,
                            batch_size=_CHUNK_SIZE,
                            execute_options=exec_opts,
                        )
                    )
                    if batches:
                        return pl.concat(batches, how="vertical_relaxed")
                    # No rows — a direct (empty) read yields the right schema.
                    return pl.read_database(statement, connection=conn, execute_options=exec_opts)
                return pd.read_sql_query(statement, con=conn, params=params)
        except SQLAlchemyError as exc:
            raise QueryError(f"Query failed: {exc}") from exc

    @overload
    def query_stream(
        self,
        sql: str,
        params: dict[str, Any] | None = ...,
        *,
        chunksize: int = ...,
        dataframe: Literal["pandas"],
    ) -> Iterator[pd.DataFrame]: ...
    @overload
    def query_stream(
        self,
        sql: str,
        params: dict[str, Any] | None = ...,
        *,
        chunksize: int = ...,
        dataframe: Literal["polars"],
    ) -> Iterator[pl.DataFrame]: ...
    @overload
    def query_stream(
        self,
        sql: str,
        params: dict[str, Any] | None = ...,
        *,
        chunksize: int = ...,
        dataframe: str | None = ...,
    ) -> Iterator[pd.DataFrame] | Iterator[pl.DataFrame]: ...

    def query_stream(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        chunksize: int = _CHUNK_SIZE,
        dataframe: str | None = None,
    ) -> Iterator[pd.DataFrame] | Iterator[pl.DataFrame]:
        """Run a read query and yield the result as DataFrame batches.

        Like :meth:`query`, but yields the result in batches of ``chunksize``
        rows instead of one DataFrame, so peak DataFrame memory is bounded by a
        single chunk. ADBC streams Arrow record batches from the proxy; the
        SQLAlchemy path uses a plain forward-only cursor and buffers the raw
        libpq result client-side.

        The database connection is held open for exactly as long as you iterate:
        it is released when the iterator is exhausted, when you ``break`` out /
        drop the iterator, or when you call ``.close()`` on it.

        Args:
            sql: The SELECT to run, with plain table names (as in :meth:`query`).
            params: Optional bind parameters, as in :meth:`query`.
            chunksize: Rows per yielded DataFrame (default 50,000).
            dataframe: Which DataFrame library to yield batches as —
                ``"pandas"`` or ``"polars"``. ``None`` (the default) uses the
                client's default.

        Yields:
            ``pandas.DataFrame`` or ``polars.DataFrame`` batches of up to
            ``chunksize`` rows, in order, per the resolved ``dataframe`` choice.

        Raises:
            DataAccessError: If ``chunksize`` is not a positive integer, or
                ``dataframe`` is not ``"pandas"`` or ``"polars"``.
            RequestsError: If your approved request can't be resolved.
            QueryError: If executing the query or fetching a batch fails.

        Example:
            >>> total = 0.0
            >>> for chunk in client.query_stream("SELECT value_as_number FROM measurement"):
            ...     total += chunk["value_as_number"].sum()
        """
        engine = self._resolve_dataframe(dataframe)
        if not isinstance(chunksize, int) or isinstance(chunksize, bool) or chunksize <= 0:
            raise DataAccessError("chunksize must be a positive integer")

        sql = qualify_statement(sql, self._ensure_access().schema)
        if self._driver == "adbc" and params:
            raise QueryError("ADBC queries do not yet support bind parameters through this proxy")
        statement: Any = text(sql) if params else sql

        def _stream() -> Iterator[Any]:
            try:
                if self._driver == "adbc":
                    for batch in self.query_arrow_stream(sql):
                        for offset in range(0, batch.num_rows, chunksize):
                            chunk = batch.slice(offset, chunksize)
                            if engine == "polars":
                                yield pl.from_arrow(opaque_to_storage(chunk))
                            else:
                                yield chunk.to_pandas()
                    return
                with self.connect() as conn:
                    # Plain forward-only cursor — NOT stream_results. The proxy
                    # rejects psycopg2's server-side cursor ("cursor can only scan
                    # forward, 55000"). iter_batches / chunksize still pull the
                    # result in fetchmany(chunksize) batches, so each yielded frame
                    # holds at most one chunk of Python rows. (libpq buffers the
                    # full result client-side; true server-side streaming would
                    # need proxy-side cursor support.)
                    if engine == "polars":
                        yield from pl.read_database(
                            statement,
                            connection=conn,
                            iter_batches=True,
                            batch_size=chunksize,
                            execute_options={"parameters": params} if params else None,
                        )
                    else:
                        yield from pd.read_sql_query(
                            statement, con=conn, params=params, chunksize=chunksize
                        )
            except SQLAlchemyError as exc:
                raise QueryError(f"Query failed: {exc}") from exc

        return _stream()
