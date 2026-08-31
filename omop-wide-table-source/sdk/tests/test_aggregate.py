from __future__ import annotations

import polars as pl
import pytest

from cbr_data_access.aggregate import (
    _coerce_wide_numerics,
    _COHORT_MAPPINGS_SQL,
    _fetch_cohort_names,
    _finalize_dataset_frames,
    _long_sql_for,
    _resolve_cohort_id,
)

_COHORT_NAMES = {101: "SANSCOG", 102: "TLSA"}


def test_cohort_mappings_sql_selects_every_active_cohort() -> None:
    assert "FROM cohort_mappings" in _COHORT_MAPPINGS_SQL
    assert "is_active" in _COHORT_MAPPINGS_SQL
    # Any organization's cohorts are exportable; the registry is scoped by the
    # access request, not by an SDK-side org filter.
    assert "org_name" not in _COHORT_MAPPINGS_SQL


def test_fetch_cohort_names_reads_the_registry_through_the_client() -> None:
    class FakeClient:
        def query(self, sql: str, *, dataframe: str) -> pl.DataFrame:
            assert dataframe == "polars"
            assert "cohort_mappings" in sql
            return pl.DataFrame({"cohort_id": [101, 102], "cohort_name": ["SANSCOG", "TLSA"]})

    assert _fetch_cohort_names(FakeClient()) == _COHORT_NAMES


def test_fetch_cohort_names_wraps_query_failures() -> None:
    class FailingClient:
        def query(self, sql: str, *, dataframe: str) -> pl.DataFrame:
            raise RuntimeError("relation does not exist")

    with pytest.raises(RuntimeError, match="cohort_mappings"):
        _fetch_cohort_names(FailingClient())


def test_resolve_cohort_id_accepts_ids_id_strings_and_registry_names() -> None:
    assert _resolve_cohort_id(101, _COHORT_NAMES) == 101
    assert _resolve_cohort_id("102", _COHORT_NAMES) == 102
    assert _resolve_cohort_id("SANSCOG", _COHORT_NAMES) == 101
    assert _resolve_cohort_id("TLSA", _COHORT_NAMES) == 102
    with pytest.raises(ValueError, match="Unknown cohort name"):
        _resolve_cohort_id("Gryffindor", _COHORT_NAMES)


def test_long_sql_uses_canonical_typed_value_columns() -> None:
    sql = " ".join(_long_sql_for(101).split())

    assert "m.value_as_number::text AS field_value" in sql
    assert "o.value_as_string AS field_value" in sql
    assert "COALESCE(m.value_source_value" not in sql
    assert "COALESCE(o.value_source_value" not in sql


def test_long_sql_defaults_to_source_field_name_with_description_fallback() -> None:
    sql = " ".join(_long_sql_for(101).split())

    expression = "COALESCE(NULLIF(source_field_name, ''), source_field_description)"
    assert f"{expression} AS field_label" in sql
    assert f"AND {expression} IS NOT NULL" in sql


def test_long_sql_can_prefer_source_field_description() -> None:
    sql = " ".join(_long_sql_for(101, "source_field_description").split())

    expression = "COALESCE(NULLIF(source_field_description, ''), source_field_name)"
    assert f"{expression} AS field_label" in sql
    assert f"AND {expression} IS NOT NULL" in sql


def test_long_sql_rejects_unknown_source_field() -> None:
    with pytest.raises(ValueError, match="source_field must be one of"):
        _long_sql_for(101, "unsafe_column")  # type: ignore[arg-type]


def test_coerce_wide_numerics_casts_only_lossless_fields() -> None:
    frame = pl.DataFrame(
        {
            "Barcode": ["001", "002", "003", "004"],
            "numeric": ["1", "-2.5", "1e3", None],
            "mixed": ["1", "Unknown", None, "3"],
            "blank": ["1", "", None, "3"],
            "empty": pl.Series([None, None, None, None], dtype=pl.String),
        }
    )

    result = _coerce_wide_numerics(frame, ["Barcode"])

    assert result.schema["Barcode"] == pl.String
    assert result.schema["numeric"] == pl.Float64
    assert result["numeric"].to_list() == [1.0, -2.5, 1000.0, None]
    assert result.schema["mixed"] == pl.String
    assert result["mixed"].to_list() == ["1", "Unknown", None, "3"]
    assert result.schema["blank"] == pl.String
    assert result.schema["empty"] == pl.String


def test_finalize_infers_after_all_cohorts_are_merged() -> None:
    datasets = {
        "Clinical": pl.DataFrame(
            {
                "Cohort": ["SANSCOG", "TLSA"],
                "Barcode": ["A", "B"],
                "Visit": ["V1", "V1"],
                "numeric_all_cohorts": ["1", "2.5"],
                "mixed_across_cohorts": ["1", "Unknown"],
            }
        )
    }
    index_cols = ["Cohort", "Barcode", "Visit"]

    result = _finalize_dataset_frames(datasets, index_cols)["Clinical"]

    assert result.schema["numeric_all_cohorts"] == pl.Float64
    assert result.schema["mixed_across_cohorts"] == pl.String
    assert result["Cohort"].to_list() == ["SANSCOG", "TLSA"]
