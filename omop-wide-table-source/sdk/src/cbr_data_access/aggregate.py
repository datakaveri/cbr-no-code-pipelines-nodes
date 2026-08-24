"""Aggregate OMOP cohort data into wide-format tables (one per dataset).

Runs **through** a :class:`~cbr_data_access.client.DataAccessClient` — i.e. through
the access-control proxy — not a direct database engine. Every table is referenced
by its plain name (``person``, ``measurement``, ``local_concept``, ...); the client
qualifies each to the request's ``req_<id>.<table>`` relation.

Because the proxy unions all of a request's cohorts into each relation (tagged with
a synthetic ``cohort_id`` column), every base table is pinned to ONE cohort with a
literal ``cohort_id = <id>`` WHERE filter. The joins deliberately do NOT re-match
``cohort_id``, and the small identity CTEs are MATERIALIZED: Postgres cannot see
base-table statistics through the proxy's rewrite, so instead of fixing the
estimates the query makes hash joins the only physically possible plan shape
(details on ``_LONG_SQL_TEMPLATE``).

Memory model: each cohort's long-format pull is spooled to temporary Parquet
part files on disk (one zstd-compressed part per fetch batch), and every
transform (dedupe, pivot) runs off that spool with polars' lazy streaming
engine. The cohort's long table is therefore never held in memory in
one piece — peak RAM is one dataset's deduped rows plus its wide result, not the
whole pull. This is what keeps large cohorts from swapping/OOMing mid-transform.

The result is ``{dataset_name: wide_dataframe}`` — one entry per
``local_concept.dataset`` — each shaped:

    Cohort | Barcode | Subject_ID | Gender | Visit | Assessment_Date |
    Age_at_Visit | <field_name_1> | <field_name_2> | ...

Usage::

    from cbr_data_access import DataAccessClient
    with DataAccessClient() as client:
        datasets = client.aggregate()              # all granted cohorts
"""

from __future__ import annotations

import os
import shutil
import tempfile
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Literal, cast

import polars as pl
from sqlalchemy import text

from ._arrow import opaque_to_storage
from ._sql import qualify_statement

# OMOP standard gender concept ids (CASE-matched in the query).
GENDER_CONCEPT_ID_MALE = 8507
GENDER_CONCEPT_ID_FEMALE = 8532

# Cohort registry: id -> display name for the exported ``Cohort`` column,
# read at aggregate time from the request's ``cohort_mappings`` table. Only
# rows with ``is_active`` and this organization participate in aggregation
# (currently 101 SANSCOG and 102 TLSA). This replaces the old hardcoded
# ``{101: "SANSCOG", 102: "TLSA"}`` map, so a newly activated cohort exports
# under its proper name without an SDK release.
COHORT_ORG_NAME = "Centre for Brain Research"


def _cohort_mappings_sql(org_name: str = COHORT_ORG_NAME) -> str:
    """Registry SELECT with the org inlined as an escaped string literal.

    A literal rather than a bind parameter keeps the query valid on the ADBC
    path, which accepts no bind parameters through the proxy.
    """
    literal = org_name.replace("'", "''")
    return (
        "SELECT cohort_id, cohort_name FROM cohort_mappings "
        f"WHERE is_active AND org_name = '{literal}'"
    )


def _fetch_cohort_names(client: object) -> dict[int, str]:
    """Read the active cohort registry (id -> name) through the client/proxy.

    Raises:
        RuntimeError: If ``cohort_mappings`` cannot be queried (e.g. the table
            is not granted by the access request).
    """
    try:
        frame = client.query(_cohort_mappings_sql(), dataframe="polars")  # type: ignore[attr-defined]
    except Exception as exc:
        raise RuntimeError(
            f"Could not read the cohort_mappings table "
            f"(is it granted by your access request?): {exc}"
        ) from exc
    return {
        int(cohort_id): str(name)
        for cohort_id, name in zip(frame["cohort_id"], frame["cohort_name"], strict=True)
        if cohort_id is not None and name is not None
    }


def _resolve_cohort_id(cohort: int | str, cohort_names: dict[int, str]) -> int:
    """Accept a cohort as an id (``101``) or an active registry name (``"SANSCOG"``)."""
    if isinstance(cohort, str) and not cohort.strip().isdigit():
        name = cohort.strip()
        ids_by_name = {n: i for i, n in cohort_names.items()}
        if name not in ids_by_name:
            raise ValueError(
                f"Unknown cohort name {name!r}; active cohorts: {sorted(ids_by_name)}"
            )
        return ids_by_name[name]
    return int(cohort)


# Identity columns carried through to every wide sheet (the pivot index).
_INDEX_COLS = [
    "Barcode",
    "Subject_ID",
    "Gender",
    "Age_at_Visit",
    "Visit",
    "Assessment_Date",
]

# Sentinel replaced (not str.format — the SQL may contain literal braces) with an
# int cohort id by _long_sql_for. The id comes from AccessRequest.cohorts, so it is
# always an int — injection-safe.
_COHORT_SENTINEL = "__COHORT__"

# Column used first when deriving wide-sheet headers. Only these fixed choices
# are accepted because SQL identifiers cannot be bind parameters.
SourceField = Literal["source_field_name", "source_field_description"]
_SOURCE_FIELDS: tuple[SourceField, SourceField] = (
    "source_field_name",
    "source_field_description",
)
_FIELD_LABEL_SENTINEL = "__FIELD_LABEL__"


def _check_source_field(source_field: str) -> SourceField:
    """Validate a local-concept column before interpolating it into SQL."""
    if source_field not in _SOURCE_FIELDS:
        choices = ", ".join(repr(field) for field in _SOURCE_FIELDS)
        raise ValueError(f"source_field must be one of: {choices}")
    return cast(SourceField, source_field)

# Long-format pull, written for the proxy: plain table names (qualified by the
# client to req_<id>.*). EVERY base table carries its own literal
# `cohort_id = <id>` WHERE filter: a literal lets Postgres prune the proxy's
# `UNION ALL` branch at plan time (a bind param can't), so each per-cohort query
# scans only its own schema — and on cmp_participants the filter also keeps a
# barcode present in both cohorts from duplicating rows. Those filters make
# cohort_id JOIN conditions redundant, and the joins must NOT match on cohort_id
# (a synthetic constant with no column statistics — joining on it explodes
# cardinality estimates). The estimates stay wrong no matter how the joins are
# phrased (Postgres cannot see base-table statistics through the rewrite's UNION
# ALL subqueries), so person_base and visit_base are MATERIALIZED: a materialized
# CTE has no indexes, per-row nested-loop probes into it are impossible, and the
# planner must hash these small tables and stream the long rows through them once
# (per-row index-probe plans measured 38-48s vs the ~10-12s hash-join floor).
# Keep this query unsorted:
# final row/column ordering happens in Polars after pivoting, avoiding a large
# database ORDER BY over millions of long rows. No concept-id filtering here:
# the proxy admits only curated questionnaire concepts into req_<id>.* (both the
# row filters and local_concept), so whatever it serves is exportable.
_LONG_SQL_TEMPLATE = """
WITH concept_map AS (
    -- Wide-sheet column headers use the selected local-concept field, falling
    -- back to the other field when the selected value is missing/empty.
    SELECT
        concept_id,
        __FIELD_LABEL__ AS field_label,
        dataset
    FROM local_concept
    WHERE cohort = __COHORT__
      AND concept_id IS NOT NULL
      AND __FIELD_LABEL__ IS NOT NULL
),
person_base AS MATERIALIZED (
    SELECT
        p.person_id,
        p.person_source_value                                         AS barcode,
        cp.subject_id,
        CASE p.gender_concept_id
            WHEN :gender_male   THEN 'Male'
            WHEN :gender_female THEN 'Female'
            ELSE 'Unknown'
        END                                                           AS gender,
        MAKE_DATE(
            p.year_of_birth,
            COALESCE(p.month_of_birth, 1),
            COALESCE(p.day_of_birth,   1)
        )                                                             AS dob
    FROM person p
    JOIN cmp_participants cp
      ON cp.barcode = p.person_source_value
    WHERE p.cohort_id = __COHORT__
      AND cp.cohort_id = __COHORT__
),
visit_base AS MATERIALIZED (
    SELECT
        vo.visit_occurrence_id,
        vo.person_id,
        vo.visit_source_value  AS visit_label,
        vo.visit_start_date    AS assessment_date
    FROM visit_occurrence vo
    WHERE vo.cohort_id = __COHORT__
),
meas_long AS (
    SELECT
        m.cohort_id,
        m.person_id,
        m.visit_occurrence_id,
        cm.dataset,
        cm.field_label                                                AS field_name,
        m.value_as_number::text                                       AS field_value
    FROM measurement m
    JOIN concept_map cm
      ON cm.concept_id = m.measurement_source_concept_id
    WHERE m.visit_occurrence_id IS NOT NULL
      AND m.cohort_id = __COHORT__
),
obs_long AS (
    SELECT
        o.cohort_id,
        o.person_id,
        o.visit_occurrence_id,
        cm.dataset,
        cm.field_label                                                AS field_name,
        o.value_as_string                                             AS field_value
    FROM observation o
    JOIN concept_map cm
      ON cm.concept_id = o.observation_source_concept_id
    WHERE o.visit_occurrence_id IS NOT NULL
      AND o.cohort_id = __COHORT__
),
all_long AS (
    SELECT * FROM meas_long
    UNION ALL
    SELECT * FROM obs_long
)
SELECT
    al.cohort_id                                                      AS "Cohort",
    pb.barcode                                                        AS "Barcode",
    pb.subject_id                                                     AS "Subject_ID",
    pb.gender                                                         AS "Gender",
    EXTRACT(YEAR FROM AGE(vb.assessment_date, pb.dob))::int           AS "Age_at_Visit",
    vb.visit_label                                                    AS "Visit",
    vb.assessment_date                                                AS "Assessment_Date",
    al.dataset,
    al.field_name,
    al.field_value
FROM all_long al
JOIN visit_base  vb
  ON vb.visit_occurrence_id = al.visit_occurrence_id
JOIN person_base pb
  ON pb.person_id = al.person_id
"""


def _long_sql_for(
    cohort: int,
    source_field: SourceField = "source_field_name",
) -> str:
    """Return long-format SQL using the selected field first for column labels."""
    source_field = _check_source_field(source_field)
    fallback = next(field for field in _SOURCE_FIELDS if field != source_field)
    field_label = f"COALESCE(NULLIF({source_field}, ''), {fallback})"
    return (
        _LONG_SQL_TEMPLATE.replace(_COHORT_SENTINEL, str(int(cohort)))
        .replace(_FIELD_LABEL_SENTINEL, field_label)
    )


ProgressReporter = Callable[[str], None]


def _noop_progress(message: str) -> None:
    _ = message


def _progress_reporter(progress: bool | ProgressReporter) -> ProgressReporter:
    if callable(progress):
        return progress
    if progress:
        return lambda message: print(message, flush=True)
    return _noop_progress


def _frame_mb(df: pl.DataFrame) -> str:
    try:
        return f"{df.estimated_size('mb'):.1f}MB"
    except Exception:
        return "?MB"


# Rows per server fetch — bounds how many Python Row objects are alive at once.
_FETCH_CHUNK = 50_000

# Long-pull columns that aren't plain text; anything not listed here spools as
# String (the query projects everything else as text). Parquet keeps these
# types end-to-end, so nothing needs re-casting on scan.
_SPOOL_DTYPES = {
    "Cohort": pl.Int64,
    "Age_at_Visit": pl.Int64,
    "Assessment_Date": pl.Date,
}

_LONG_COLUMNS = [
    "Cohort",
    "Barcode",
    "Subject_ID",
    "Gender",
    "Age_at_Visit",
    "Visit",
    "Assessment_Date",
    "dataset",
    "field_name",
    "field_value",
]


def _long_spool_schema() -> pl.Schema:
    """Return the stable schema shared by SQLAlchemy and ADBC spool parts."""
    return pl.Schema({name: _SPOOL_DTYPES.get(name, pl.String) for name in _LONG_COLUMNS})


def _adbc_long_sql_for(
    cohort: int,
    *,
    gender_male: int,
    gender_female: int,
    source_field: SourceField = "source_field_name",
) -> str:
    """Render the aggregate query without binds for the proxy's ADBC path.

    These values are concept identifiers, so converting them to ``int`` before
    rendering both preserves the existing API and prevents SQL injection.
    """
    return (
        _long_sql_for(cohort, source_field)
        .replace(":gender_male", str(int(gender_male)))
        .replace(":gender_female", str(int(gender_female)))
    )


def _spool_long_parquet(
    client: object,
    *,
    cohort: int,
    gender_male: int,
    gender_female: int,
    source_field: SourceField = "source_field_name",
    report: ProgressReporter = _noop_progress,
) -> str:
    """Run the long-format pull for ONE cohort and spool it to Parquet parts.

    With the SQLAlchemy driver, fetches with a plain forward-only cursor
    (proxy-safe; the proxy rejects server-side cursors with "cursor can only
    scan forward, 55000"). With ADBC, consumes native Arrow record batches.
    Both paths write zstd-compressed Parquet parts into a temporary directory.
    On success the CALLER owns that directory and must delete it; on any error
    it is removed here before re-raising.

    psycopg2's default cursor buffers the whole result client-side inside
    ``execute()`` — that transfer dominates this phase's wall-clock and raw
    memory; the spool loop itself only holds one fetch batch of Python rows.
    """
    # Spool dir in the system temp dir (/tmp on Linux; set TMPDIR to move it
    # if /tmp is a small tmpfs and the pull is huge).
    spool_dir = tempfile.mkdtemp(prefix=f"cbr_long_{cohort}_")
    try:
        spool_start = time.perf_counter()
        rows = 0
        parts = 0
        last_report = 0
        driver = client.driver  # type: ignore[attr-defined]
        if driver == "adbc":
            # query_arrow_stream qualifies the plain relation names against the
            # selected request. Split driver batches to retain the established
            # 50k-row spool bound even if the ADBC driver returns a larger batch.
            sql = _adbc_long_sql_for(
                cohort,
                gender_male=gender_male,
                gender_female=gender_female,
                source_field=source_field,
            )
            spool_schema = _long_spool_schema()
            report(f"aggregate ADBC stream start cohort={cohort}")
            for batch in client.query_arrow_stream(sql):  # type: ignore[attr-defined]
                for offset in range(0, batch.num_rows, _FETCH_CHUNK):
                    chunk = batch.slice(offset, _FETCH_CHUNK)
                    frame = cast(pl.DataFrame, pl.from_arrow(opaque_to_storage(chunk)))
                    frame.cast(spool_schema, strict=False).write_parquet(
                        os.path.join(spool_dir, f"part_{parts:05d}.parquet"),
                        compression="zstd",
                    )
                    parts += 1
                    rows += chunk.num_rows
                    if rows - last_report >= 500_000:
                        last_report = rows
                        report(
                            f"aggregate spool progress cohort={cohort} "
                            f"rows={rows} "
                            f"seconds={time.perf_counter() - spool_start:.1f}"
                        )
            if parts == 0:
                pl.DataFrame(schema=spool_schema).write_parquet(
                    os.path.join(spool_dir, "part_00000.parquet"),
                    compression="zstd",
                )
            report(
                f"aggregate ADBC stream done cohort={cohort} "
                f"seconds={time.perf_counter() - spool_start:.1f}"
            )
        else:
            request_schema = client._ensure_access().schema  # type: ignore[attr-defined]
            sql = qualify_statement(_long_sql_for(cohort, source_field), request_schema)
            params = {"gender_male": gender_male, "gender_female": gender_female}
            with client.connect() as conn:  # type: ignore[attr-defined]
                report(f"aggregate query execute start cohort={cohort}")
                result = conn.execute(text(sql), params)
                report(
                    f"aggregate query execute done cohort={cohort} "
                    f"seconds={time.perf_counter() - spool_start:.1f}"
                )
                spool_schema = pl.Schema(
                    {key: _SPOOL_DTYPES.get(key, pl.String) for key in result.keys()}
                )
                while True:
                    batch = result.fetchmany(_FETCH_CHUNK)
                    if not batch:
                        break
                    pl.DataFrame(
                        list(map(tuple, batch)),
                        schema=spool_schema,
                        orient="row",
                        strict=False,
                    ).write_parquet(
                        os.path.join(spool_dir, f"part_{parts:05d}.parquet"),
                        compression="zstd",
                    )
                    parts += 1
                    rows += len(batch)
                    if rows - last_report >= 500_000:
                        last_report = rows
                        report(
                            f"aggregate spool progress cohort={cohort} "
                            f"rows={rows} "
                            f"seconds={time.perf_counter() - spool_start:.1f}"
                        )
                if parts == 0:
                    # Empty pull: still write one typed, empty part so the scan
                    # glob resolves and downstream sees the columns.
                    pl.DataFrame(schema=spool_schema).write_parquet(
                        os.path.join(spool_dir, "part_00000.parquet"),
                        compression="zstd",
                    )
        report(
            f"aggregate spool done cohort={cohort} "
            f"seconds={time.perf_counter() - spool_start:.1f} rows={rows}"
        )
        return spool_dir
    except BaseException:
        shutil.rmtree(spool_dir, ignore_errors=True)
        raise


def _scan_long_spool(spool_dir: str, cohort_names: dict[int, str]) -> pl.LazyFrame:
    """Lazily scan a spooled long pull (directory of Parquet part files).

    Column types are already right in the spool; the only transform is swapping
    the numeric ``Cohort`` id for its ``cohort_names`` registry name. Returns a
    LazyFrame so downstream dedupe/pivot can run on the streaming engine
    without materializing the whole spool.
    """
    lf = pl.scan_parquet(os.path.join(spool_dir, "*.parquet"))
    name_by_id = {str(i): name for i, name in cohort_names.items()}
    if name_by_id and "Cohort" in lf.collect_schema().names():
        lf = lf.with_columns(pl.col("Cohort").cast(pl.String).replace(name_by_id))
    return lf


def _collect_streaming(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a lazy plan with the streaming engine (bounded memory)."""
    try:
        return lf.collect(engine="streaming")
    except TypeError:  # polars too old for the engine= keyword
        return lf.collect(streaming=True)  # type: ignore[call-overload]


def _fetch_long_one(
    client: object,
    *,
    cohort: int,
    gender_male: int,
    gender_female: int,
    source_field: SourceField = "source_field_name",
    report: ProgressReporter = _noop_progress,
) -> pl.DataFrame:
    """Run the long-format pull for ONE cohort and return it as a polars frame.

    Spools to temporary Parquet parts (see :func:`_spool_long_parquet`), then
    collects them in one shot. This materializes the whole long table — fine
    for modest cohorts and ad-hoc use; the aggregate pipeline itself pivots
    straight off the spool instead (see :func:`_fetch_pivoted_cohort`) so it
    never pays this memory cost.
    """
    cohort_names = _fetch_cohort_names(client)
    spool_dir = _spool_long_parquet(
        client,
        cohort=cohort,
        gender_male=gender_male,
        gender_female=gender_female,
        source_field=source_field,
        report=report,
    )
    try:
        parse_start = time.perf_counter()
        frame = _scan_long_spool(spool_dir, cohort_names).collect()
        report(
            f"aggregate parse done cohort={cohort} "
            f"seconds={time.perf_counter() - parse_start:.1f} shape={frame.shape}"
        )
        return frame
    finally:
        shutil.rmtree(spool_dir, ignore_errors=True)


def _dataset_key(dataset_name: object) -> str:
    """Normalize a local_concept.dataset value into the public result key."""
    return str(dataset_name) if dataset_name else "Unknown"


def _select_wide_columns(wide: pl.DataFrame, index_cols: list[str]) -> pl.DataFrame:
    field_cols = sorted(c for c in wide.columns if c not in index_cols)
    return wide.select([c for c in index_cols if c in wide.columns] + field_cols)


def _sort_wide_rows(wide: pl.DataFrame) -> pl.DataFrame:
    # Match the reference export's final row order: Barcode, then Visit.
    sort_cols = [c for c in ("Barcode", "Visit") if c in wide.columns]
    return wide.sort(sort_cols) if sort_cols else wide


def _coerce_wide_numerics(wide: pl.DataFrame, index_cols: list[str]) -> pl.DataFrame:
    """Cast fully numeric pivot fields to Float64 without losing values.

    Aggregate values arrive as strings because measurement and observation
    share one long-format ``field_value`` column. Once that column has been
    pivoted, each field can have its own dtype. A field is converted only when
    it contains at least one non-null value and a non-strict float cast adds no
    new nulls; mixed/categorical and entirely-null fields remain String.
    """
    numeric_fields: list[pl.Series] = []
    for name in wide.columns:
        if name in index_cols:
            continue
        source = wide.get_column(name)
        if len(source) == source.null_count():
            continue
        numeric = source.cast(pl.Float64, strict=False)
        if numeric.null_count() == source.null_count():
            numeric_fields.append(numeric)
    return wide.with_columns(*numeric_fields) if numeric_fields else wide


def _dataset_names(lf: pl.LazyFrame) -> list[object]:
    """Distinct ``local_concept.dataset`` values in the scan (nulls included)."""
    names = (
        _collect_streaming(lf.select(pl.col("dataset").unique())).get_column("dataset").to_list()
    )
    return sorted(names, key=_dataset_key)


def _pivot_one_dataset(
    lf: pl.LazyFrame,
    index_cols: list[str],
    *,
    dataset_value: object,
    name: str,
    report: ProgressReporter,
) -> pl.DataFrame:
    start = time.perf_counter()
    report(f"aggregate pivot dataset start name={name}")
    predicate = (
        pl.col("dataset").is_null() if dataset_value is None else pl.col("dataset") == dataset_value
    )
    # Streaming dedupe straight off the on-disk spool: peak memory is this one
    # dataset's deduped long rows, never the cohort's whole long table.
    deduped = _collect_streaming(
        lf.filter(predicate)
        .group_by([*index_cols, "field_name"])
        .agg(pl.col("field_value").first())
    )
    report(
        f"aggregate dedupe done name={name} "
        f"seconds={time.perf_counter() - start:.1f} "
        f"shape={deduped.shape} size={_frame_mb(deduped)}"
    )
    wide = deduped.pivot(index=index_cols, on="field_name", values="field_value")
    selected = _select_wide_columns(wide, index_cols)
    report(
        f"aggregate pivot dataset done name={name} "
        f"seconds={time.perf_counter() - start:.1f} "
        f"shape={selected.shape} size={_frame_mb(selected)}"
    )
    return selected


def _pivot_datasets(
    lf: pl.LazyFrame,
    index_cols: list[str],
    *,
    report: ProgressReporter = _noop_progress,
) -> dict[str, pl.DataFrame]:
    """Pivot each ``local_concept.dataset`` in the scan to a wide frame.

    Datasets are pivoted one at a time, each re-scanning the spool with the
    streaming engine — trading a little disk I/O per dataset for a memory peak
    of one dataset instead of the whole cohort. Final row sorting happens once
    in :func:`_finalize_dataset_frames`.
    """
    datasets: dict[str, pl.DataFrame] = {}
    for value in _dataset_names(lf):
        name = _dataset_key(value)
        datasets[name] = _pivot_one_dataset(
            lf, index_cols, dataset_value=value, name=name, report=report
        )
    return datasets


def _merge_dataset_frames(
    target: dict[str, pl.DataFrame],
    incoming: dict[str, pl.DataFrame],
    report: ProgressReporter = _noop_progress,
) -> None:
    """Merge per-cohort wide frames without rebuilding the long-format table."""
    for name, df in incoming.items():
        start = time.perf_counter()
        if name in target:
            target[name] = pl.concat([target[name], df], how="diagonal_relaxed")
        else:
            target[name] = df
        report(
            f"aggregate merge done name={name} "
            f"seconds={time.perf_counter() - start:.1f} "
            f"shape={target[name].shape} size={_frame_mb(target[name])}"
        )


def _finalize_dataset_frames(
    datasets: dict[str, pl.DataFrame],
    index_cols: list[str],
    report: ProgressReporter = _noop_progress,
) -> dict[str, pl.DataFrame]:
    finalized: dict[str, pl.DataFrame] = {}
    for name, df in datasets.items():
        start = time.perf_counter()
        report(f"aggregate finalize start name={name} shape={df.shape} size={_frame_mb(df)}")
        ordered = _select_wide_columns(_sort_wide_rows(df), index_cols)
        finalized[name] = _coerce_wide_numerics(ordered, index_cols)
        report(
            f"aggregate finalize done name={name} "
            f"seconds={time.perf_counter() - start:.1f} "
            f"shape={finalized[name].shape} size={_frame_mb(finalized[name])}"
        )
    return finalized


def _fetch_pivoted_cohort(
    client: object,
    *,
    cohort: int,
    cohort_names: dict[int, str],
    gender_male: int,
    gender_female: int,
    source_field: SourceField,
    index_cols: list[str],
    drop_cohort: bool,
    report: ProgressReporter,
) -> dict[str, pl.DataFrame]:
    """Spool one cohort's long pull to disk, then pivot it dataset-by-dataset.

    The cohort's long table never materializes in memory: rows stream from the
    server into temporary Parquet parts, and each dataset's dedupe+pivot
    re-scans that spool with polars' streaming engine. The spool is deleted
    once every dataset is pivoted.
    """
    start = time.perf_counter()
    report(f"aggregate fetch start cohort={cohort}")
    spool_dir = _spool_long_parquet(
        client,
        cohort=cohort,
        gender_male=gender_male,
        gender_female=gender_female,
        source_field=source_field,
        report=report,
    )
    try:
        lf = _scan_long_spool(spool_dir, cohort_names)
        if drop_cohort:
            lf = lf.drop("Cohort")
        datasets = _pivot_datasets(lf, index_cols, report=report)
        report(
            f"aggregate cohort done cohort={cohort} "
            f"seconds={time.perf_counter() - start:.1f} datasets={len(datasets)}"
        )
        return datasets
    finally:
        shutil.rmtree(spool_dir, ignore_errors=True)


def _aggregate_cohorts(
    client: object,
    cohorts: list[int],
    *,
    cohort_names: dict[int, str],
    gender_male: int,
    gender_female: int,
    source_field: SourceField,
    index_cols: list[str],
    drop_cohort: bool,
    max_workers: int,
    progress: bool | ProgressReporter,
) -> dict[str, pl.DataFrame]:
    """Fetch/pivot cohorts incrementally to avoid one huge concatenated long frame."""
    report = _progress_reporter(progress)
    total_start = time.perf_counter()
    client._ensure_access()  # type: ignore[attr-defined]
    driver = client.driver  # type: ignore[attr-defined]
    if driver == "sqlalchemy":
        client._get_engine()  # type: ignore[attr-defined]

    worker_count = max(1, min(int(max_workers), len(cohorts)))
    report(f"aggregate start driver={driver} cohorts={cohorts} workers={worker_count}")
    datasets: dict[str, pl.DataFrame] = {}

    if worker_count == 1:
        for cohort in cohorts:
            try:
                _merge_dataset_frames(
                    datasets,
                    _fetch_pivoted_cohort(
                        client,
                        cohort=cohort,
                        cohort_names=cohort_names,
                        gender_male=gender_male,
                        gender_female=gender_female,
                        source_field=source_field,
                        index_cols=index_cols,
                        drop_cohort=drop_cohort,
                        report=report,
                    ),
                    report=report,
                )
            except Exception as exc:  # noqa: BLE001 - re-raised with cohort context
                raise RuntimeError(f"cohort {cohort} export failed: {exc}") from exc
        finalized = _finalize_dataset_frames(datasets, index_cols, report=report)
        report(f"aggregate done seconds={time.perf_counter() - total_start:.1f}")
        return finalized

    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = {
            pool.submit(
                _fetch_pivoted_cohort,
                client,
                cohort=cohort,
                cohort_names=cohort_names,
                gender_male=gender_male,
                gender_female=gender_female,
                source_field=source_field,
                index_cols=index_cols,
                drop_cohort=drop_cohort,
                report=report,
            ): cohort
            for cohort in cohorts
        }
        for future in as_completed(futures):
            cohort = futures[future]
            try:
                _merge_dataset_frames(datasets, future.result(), report=report)
            except Exception as exc:  # noqa: BLE001 - re-raised with cohort context
                raise RuntimeError(f"cohort {cohort} export failed: {exc}") from exc

    finalized = _finalize_dataset_frames(datasets, index_cols, report=report)
    report(f"aggregate done seconds={time.perf_counter() - total_start:.1f}")
    return finalized


def aggregate(
    client: object,
    cohort: int | str | None = None,
    *,
    gender_male: int = GENDER_CONCEPT_ID_MALE,
    gender_female: int = GENDER_CONCEPT_ID_FEMALE,
    source_field: SourceField = "source_field_name",
    max_workers: int = 1,
    progress: bool | ProgressReporter = False,
) -> dict[str, pl.DataFrame]:
    """Return ``{dataset_name: wide_dataframe}`` for the client's granted data.

    The long-format pull runs one query per cohort, spooled to temporary
    Parquet part files in the system temp dir (/tmp). Each dataset is then
    deduped and pivoted straight off that spool with polars' streaming engine,
    so the long table is never held in memory whole — this keeps big cohorts
    from stalling in swap. Each cohort is merged into the final per-dataset
    frames before the next one runs.
    After all cohorts are merged, pivoted fields whose non-null values all
    parse as numbers are returned as ``Float64``; mixed/categorical and
    entirely-null fields remain ``String``. Identity columns are never inferred.
    It is serial by default because each cohort query is a large DB scan/stream;
    pass ``max_workers > 1`` to opt into concurrent cohort pulls when the
    backend, client RAM, and network path have headroom.

    Args:
        client: A :class:`~cbr_data_access.client.DataAccessClient` (resolves the
            request automatically; with several approved requests pin one via
            ``client.set_access(...)`` first).
        cohort: A single cohort to export, by id (``101``) or by its name in the
            request's ``cohort_mappings`` table (e.g. ``"SANSCOG"``, ``"TLSA"``).
            The ``Cohort`` column is dropped from the sheets, and only that
            cohort is queried. ``None`` (default) exports every granted cohort
            that is active in ``cohort_mappings`` and prepends a ``Cohort``
            column carrying the cohort names.
        gender_male / gender_female: Gender concept ids used to label ``Gender``.
        source_field: Local-concept column used first for wide-sheet headers:
            ``"source_field_name"`` (default) or
            ``"source_field_description"``. Missing or empty values fall back
            to the other column.
        max_workers: Max cohorts fetched concurrently (capped at the cohort count).
            Defaults to 1 to avoid piling up heavy scans/streams.
        progress: If true, print aggregate phase timing. If callable, it receives
            each progress message. Strongly recommended for large cohorts — the
            fetch phase alone can run for minutes with no other output.

    Raises:
        RuntimeError: If ``cohort_mappings`` cannot be read, the request grants
            no active cohorts, or the query returns no rows.
        ValueError: If ``cohort`` names an unknown or inactive cohort.
    """
    source_field = _check_source_field(source_field)
    report = _progress_reporter(progress)
    # The cohort registry (id -> name) comes from the request's cohort_mappings
    # table; only cohorts active there for this org participate in aggregation.
    cohort_names = _fetch_cohort_names(client)
    # A single-cohort export drops the Cohort column (it's constant); a full
    # export keeps it as the leading identity column so same-barcode rows from
    # different cohorts stay distinct through the pivot.
    if cohort is not None:
        cohort_id = _resolve_cohort_id(cohort, cohort_names)
        if cohort_id not in cohort_names:
            raise ValueError(
                f"Cohort {cohort_id} is not an active {COHORT_ORG_NAME} cohort; "
                f"active cohorts: {sorted(cohort_names)}"
            )
        cohorts = [cohort_id]
        drop_cohort = True
        index_cols = list(_INDEX_COLS)
    else:
        granted = list(client._ensure_access().cohorts)  # type: ignore[attr-defined]
        cohorts = [c for c in granted if c in cohort_names]
        skipped = sorted(set(granted) - set(cohorts))
        if skipped:
            report(f"aggregate skipping granted but inactive cohorts: {skipped}")
        drop_cohort = False
        index_cols = ["Cohort", *_INDEX_COLS]
    if not cohorts:
        raise RuntimeError(f"The access request grants no active {COHORT_ORG_NAME} cohorts.")

    datasets = _aggregate_cohorts(
        client,
        cohorts,
        cohort_names=cohort_names,
        gender_male=gender_male,
        gender_female=gender_female,
        source_field=source_field,
        index_cols=index_cols,
        drop_cohort=drop_cohort,
        max_workers=max_workers,
        progress=report,
    )
    if not datasets:
        raise RuntimeError("No data returned for the granted cohort(s).")
    return datasets
