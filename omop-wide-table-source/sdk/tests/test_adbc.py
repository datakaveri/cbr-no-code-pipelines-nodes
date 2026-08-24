from __future__ import annotations

import shutil
import warnings
from datetime import date
from importlib import import_module
from types import SimpleNamespace

import polars as pl
import pyarrow as pa
import pytest

from cbr_data_access import DataAccessClient, DataAccessError, QueryError
from cbr_data_access import client as client_module

aggregate_module = import_module("cbr_data_access.aggregate")


def _opaque_column(
    name: str,
    storage: pa.Array,
) -> tuple[pa.Field, pa.Array]:
    """Build an opaque column across both old and new supported PyArrow versions."""
    metadata = {
        b"ARROW:extension:name": b"arrow.opaque",
        b"ARROW:extension:metadata": (
            b'{"type_name":"measurement_value","vendor_name":"PostgreSQL"}'
        ),
    }
    if hasattr(pa, "opaque"):
        opaque_type = pa.opaque(storage.type, "measurement_value", "PostgreSQL")
        return pa.field(name, opaque_type), pa.ExtensionArray.from_storage(opaque_type, storage)
    return pa.field(name, storage.type, metadata=metadata), storage


def test_adbc_uri_percent_encodes_credentials_and_ipv6() -> None:
    uri = client_module._adbc_postgresql_uri(
        {
            "username": "researcher@example.org",
            "host": "2001:db8::1",
            "port": "7543",
            "database": "tenant_researcher",
        },
        "header.payload/signature+value",
    )

    assert uri == (
        "postgresql://researcher%40example.org:"
        "header.payload%2Fsignature%2Bvalue@[2001:db8::1]:7543/"
        "tenant_researcher?sslmode=disable"
    )


def test_client_rejects_unknown_driver() -> None:
    with pytest.raises(DataAccessError, match="Unsupported driver"):
        DataAccessClient(driver="unknown")


def test_connect_adbc_uses_token_as_password(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, bool]] = []

    class FakeError(Exception):
        pass

    class FakeConnection:
        closed = False

        def close(self) -> None:
            self.closed = True

    connection = FakeConnection()

    class FakeADBC:
        Error = FakeError

        @staticmethod
        def connect(*, uri: str, autocommit: bool) -> FakeConnection:
            calls.append((uri, autocommit))
            return connection

    monkeypatch.setattr(client_module, "_load_adbc_dbapi", lambda: FakeADBC)
    client = DataAccessClient(driver="adbc")
    monkeypatch.setattr(
        client,
        "_ensure_connection_info",
        lambda: {
            "username": "user",
            "host": "proxy.example.org",
            "port": "7543",
            "database": "tenant_user",
        },
    )
    monkeypatch.setattr(client, "_ensure_token", lambda: "token/with+symbols")

    with client.connect_adbc() as got:
        assert got is connection

    assert calls == [
        (
            "postgresql://user:token%2Fwith%2Bsymbols@"
            "proxy.example.org:7543/tenant_user?sslmode=disable",
            True,
        )
    ]
    assert connection.closed


def test_adbc_query_returns_requested_dataframe(monkeypatch: pytest.MonkeyPatch) -> None:
    client = DataAccessClient(driver="adbc")
    monkeypatch.setattr(client, "_ensure_access", lambda: SimpleNamespace(schema="req_123"))
    monkeypatch.setattr(
        client,
        "_execute_adbc_arrow",
        lambda sql: pa.table({"person_id": [1, 2], "label": ["a", "b"]}),
    )

    pandas_result = client.query("SELECT person_id, label FROM person")
    polars_result = client.query("SELECT person_id, label FROM person", dataframe="polars")

    assert pandas_result["person_id"].tolist() == [1, 2]
    assert polars_result["label"].to_list() == ["a", "b"]


def test_adbc_polars_query_loads_opaque_columns_as_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = DataAccessClient(driver="adbc")
    monkeypatch.setattr(client, "_ensure_access", lambda: SimpleNamespace(schema="req_123"))
    opaque_field, opaque_values = _opaque_column(
        "value_as_opaque", pa.array([b"one", None, b"three"])
    )
    table = pa.Table.from_arrays(
        [opaque_values, pa.array([1, 2, 3])],
        schema=pa.schema([opaque_field, pa.field("person_id", pa.int64())]),
    )
    monkeypatch.setattr(client, "_execute_adbc_arrow", lambda sql: table)

    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        result = client.query(
            "SELECT value_as_opaque, person_id FROM measurement",
            dataframe="polars",
        )

    assert result.schema == {"value_as_opaque": pl.Binary, "person_id": pl.Int64}
    assert result["value_as_opaque"].to_list() == [b"one", None, b"three"]


def test_adbc_polars_stream_loads_opaque_columns_as_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = DataAccessClient(driver="adbc")
    monkeypatch.setattr(client, "_ensure_access", lambda: SimpleNamespace(schema="req_123"))
    opaque_field, opaque_values = _opaque_column("value_as_opaque", pa.array(["one", "two", None]))
    batch = pa.RecordBatch.from_arrays(
        [opaque_values],
        schema=pa.schema([opaque_field]),
    )
    monkeypatch.setattr(client, "query_arrow_stream", lambda sql: iter([batch]))

    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        chunks = list(
            client.query_stream(
                "SELECT value_as_opaque FROM measurement",
                chunksize=2,
                dataframe="polars",
            )
        )

    assert [chunk.schema for chunk in chunks] == [
        {"value_as_opaque": pl.String},
        {"value_as_opaque": pl.String},
    ]
    assert [value for chunk in chunks for value in chunk["value_as_opaque"]] == [
        "one",
        "two",
        None,
    ]


def test_adbc_query_rejects_bind_parameters() -> None:
    client = DataAccessClient(driver="adbc")
    client._default_request = SimpleNamespace(schema="req_123")  # type: ignore[assignment]

    with pytest.raises(QueryError, match="do not yet support bind parameters"):
        client.query("SELECT * FROM person WHERE person_id = :id", {"id": 1})


def test_adbc_stream_splits_large_arrow_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = DataAccessClient(driver="adbc")
    monkeypatch.setattr(client, "_ensure_access", lambda: SimpleNamespace(schema="req_123"))
    batch = pa.record_batch({"person_id": [1, 2, 3, 4, 5]})
    monkeypatch.setattr(client, "query_arrow_stream", lambda sql: iter([batch]))

    chunks = list(client.query_stream("SELECT * FROM person", chunksize=2))

    assert [len(chunk) for chunk in chunks] == [2, 2, 1]
    assert [value for chunk in chunks for value in chunk["person_id"]] == [1, 2, 3, 4, 5]


def test_adbc_aggregate_spools_arrow_without_sqlalchemy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = DataAccessClient(driver="adbc")
    queries: list[str] = []
    batch = pa.record_batch(
        {
            "Cohort": [101, 101, 101],
            "Barcode": ["B1", "B2", "B3"],
            "Subject_ID": ["S1", "S2", "S3"],
            "Gender": ["Male", "Female", "Unknown"],
            "Age_at_Visit": [60, 61, 62],
            "Visit": ["V1", "V1", "V2"],
            "Assessment_Date": pa.array(
                [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
                type=pa.date32(),
            ),
            "dataset": ["Clinical", "Clinical", "Blood"],
            "field_name": ["a", "b", "c"],
            "field_value": ["1", "2", "3"],
        }
    )

    def query_arrow_stream(sql: str):  # type: ignore[no-untyped-def]
        queries.append(sql)
        return iter([batch])

    def fail_connect():  # type: ignore[no-untyped-def]
        raise AssertionError("ADBC aggregate must not open a SQLAlchemy connection")

    monkeypatch.setattr(client, "query_arrow_stream", query_arrow_stream)
    monkeypatch.setattr(client, "connect", fail_connect)
    monkeypatch.setattr(aggregate_module, "_FETCH_CHUNK", 2)

    spool_dir = aggregate_module._spool_long_parquet(
        client,
        cohort=101,
        gender_male=8507,
        gender_female=8532,
    )
    try:
        frame = pl.read_parquet(f"{spool_dir}/*.parquet")
    finally:
        shutil.rmtree(spool_dir, ignore_errors=True)

    assert frame.shape == (3, 10)
    assert frame["Barcode"].to_list() == ["B1", "B2", "B3"]
    assert frame.schema["Age_at_Visit"] == pl.Int64
    assert len(queries) == 1
    assert ":gender_male" not in queries[0]
    assert ":gender_female" not in queries[0]
    assert "WHEN 8507" in queries[0]
    assert "WHEN 8532" in queries[0]


def test_adbc_aggregate_does_not_initialize_sqlalchemy_engine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = DataAccessClient(driver="adbc")
    events: list[str] = []
    monkeypatch.setattr(client, "_ensure_access", lambda: SimpleNamespace(cohorts=[101]))

    def fail_get_engine():  # type: ignore[no-untyped-def]
        raise AssertionError("ADBC aggregate must not initialize SQLAlchemy")

    monkeypatch.setattr(client, "_get_engine", fail_get_engine)
    monkeypatch.setattr(
        aggregate_module,
        "_fetch_pivoted_cohort",
        lambda *args, **kwargs: {},
    )

    result = aggregate_module._aggregate_cohorts(
        client,
        [101],
        cohort_names={101: "SANSCOG"},
        gender_male=8507,
        gender_female=8532,
        source_field="source_field_name",
        index_cols=[],
        drop_cohort=True,
        max_workers=1,
        progress=events.append,
    )

    assert result == {}
    assert events[0] == "aggregate start driver=adbc cohorts=[101] workers=1"
