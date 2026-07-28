"""Missing Value Fill (LOCF) node.

Carries each participant's last valid measurement forward across timepoints.

Rules, in order:
  1. A value outside its column's declared valid range(s) is treated as missing.
  2. Missing values take the participant's most recent valid earlier value.
  3. Rows for visits the participant never attended stay blank — LOCF fills gaps
     in observation, it does not invent visits that did not happen.

Contract:
- Reads NODE_CONTEXT (JSON env var) for node identity, inputs, outputs, config.
- Reads INPUT_S3_* / ARTIFACT_S3_* for storage.
- Writes to the EXACT paths in output.files[i].path.
- Prints a final JSON status to stdout; exits 0 on success, 1 on failure.
"""

import json
import os
import shutil
import sys
import tempfile

import boto3
import numpy as np
import pandas as pd

NODE_PREFIX = "[MISSING VALUE FILL]"


def log(message: str) -> None:
    print(f"{NODE_PREFIX} {message}", flush=True)


def log_error(message: str) -> None:
    print(f"{NODE_PREFIX} ERROR: {message}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------


def split_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {uri}")
    bucket, _, key = uri[len("s3://"):].partition("/")
    return bucket, key


def _build_s3_client(prefix: str):
    endpoint = os.environ[f"{prefix}_S3_ENDPOINT"]
    use_ssl = os.environ.get(f"{prefix}_S3_USE_SSL", "false").lower() == "true"
    if "://" not in endpoint:
        endpoint = ("https://" if use_ssl else "http://") + endpoint
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ[f"{prefix}_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ[f"{prefix}_S3_SECRET_KEY"],
        aws_session_token=os.environ.get(f"{prefix}_S3_SESSION_TOKEN") or None,
        region_name=os.environ.get(f"{prefix}_S3_REGION", "us-east-1"),
    )


_S3_CLIENTS: dict = {}


def get_s3_client_for_path(s3_path: str):
    bucket, _ = split_s3_uri(s3_path)
    prefix = "ARTIFACT" if bucket == os.environ.get("ARTIFACT_S3_BUCKET") else "INPUT"
    if prefix not in _S3_CLIENTS:
        _S3_CLIENTS[prefix] = _build_s3_client(prefix)
    return _S3_CLIENTS[prefix]


def read_table(path: str) -> pd.DataFrame:
    lowered = path.lower()
    if lowered.endswith(".parquet") or lowered.endswith(".pq"):
        return pd.read_parquet(path)
    if lowered.endswith(".csv"):
        return pd.read_csv(path)
    if lowered.endswith(".tsv"):
        return pd.read_csv(path, sep="\t")
    if lowered.endswith(".xlsx") or lowered.endswith(".xls"):
        return pd.read_excel(path)
    raise ValueError(
        f"Unsupported input format: {os.path.basename(path)}. "
        "Expected parquet, csv, tsv, xlsx or xls."
    )


# ---------------------------------------------------------------------------
# Parameter parsing
# ---------------------------------------------------------------------------

# The source notebook declared exception ranges under the key "exceptions" but read
# them back as "exception_ranges", so the feature silently never ran. Here there is
# one canonical name and anything unrecognised is rejected rather than ignored.
RANGE_KEYS = {"ranges", "exclude_ranges"}


def _parse_intervals(raw, column: str, key: str) -> list[tuple[float, float]]:
    if raw is None:
        return []
    if not isinstance(raw, (list, tuple)):
        raise ValueError(f"parameters.{column}.{key} must be a list of [min, max] pairs")
    intervals = []
    for item in raw:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            raise ValueError(
                f"parameters.{column}.{key} must contain [min, max] pairs, got: {item!r}"
            )
        low, high = float(item[0]), float(item[1])
        if low > high:
            raise ValueError(
                f"parameters.{column}.{key}: {low} is greater than {high}"
            )
        intervals.append((low, high))
    return intervals


def parse_parameters(raw) -> dict[str, dict]:
    """Normalise the config into {column: {ranges: [...], exclude_ranges: [...]}}.

    Accepts both the explicit form and the shorthand where a column maps directly
    to its list of ranges.
    """
    if isinstance(raw, str):
        raw = json.loads(raw) if raw.strip() else {}
    if not isinstance(raw, dict) or not raw:
        raise ValueError("'Columns to fill' must be a non-empty object mapping column names to ranges")

    parsed: dict[str, dict] = {}
    for column, spec in raw.items():
        if isinstance(spec, (list, tuple)):  # shorthand: column -> ranges
            spec = {"ranges": spec}
        if not isinstance(spec, dict):
            raise ValueError(
                f"parameters.{column} must be a list of ranges or an object with a 'ranges' key"
            )
        unknown = set(spec) - RANGE_KEYS
        if unknown:
            raise ValueError(
                f"parameters.{column}: unknown key(s) {sorted(unknown)}. "
                f"Expected one of {sorted(RANGE_KEYS)}."
            )
        parsed[column] = {
            "ranges": _parse_intervals(spec.get("ranges"), column, "ranges"),
            "exclude_ranges": _parse_intervals(
                spec.get("exclude_ranges"), column, "exclude_ranges"
            ),
        }
    return parsed


# ---------------------------------------------------------------------------
# Filling
# ---------------------------------------------------------------------------


def _mask_for(values: pd.Series, intervals: list[tuple[float, float]]) -> pd.Series:
    mask = pd.Series(False, index=values.index)
    for low, high in intervals:
        mask |= (values >= low) & (values <= high)
    return mask


def to_numeric_strict(raw: pd.Series, column: str) -> pd.Series:
    """Coerce to numbers, but refuse to silently destroy a non-numeric column.

    Plain `to_numeric(errors="coerce")` turns text into NaN, so configuring a
    string column (a sex code, a site name) would blank every value and report
    it as 'still missing' rather than as a mistake.
    """
    if raw.dtype == object:
        # Empty strings are missing data, not unparseable data.
        raw = raw.mask(raw.map(lambda v: isinstance(v, str) and not v.strip()))

    numeric = pd.to_numeric(raw, errors="coerce")
    unparseable = raw.notna() & numeric.isna()
    if unparseable.any():
        examples = ", ".join(repr(v) for v in raw[unparseable].unique()[:3])
        raise ValueError(
            f"Column '{column}' is configured in 'Columns to fill' but holds "
            f"non-numeric values ({examples}). Carrying values forward is only "
            "defined for numeric measures — remove this column from the config."
        )
    return numeric


def fill_column(
    df: pd.DataFrame,
    column: str,
    spec: dict,
    participant_col: str,
    attended: pd.Series,
    max_carry: int | None,
) -> tuple[pd.Series, dict]:
    """Return the filled column and a report row. `df` must already be sorted."""
    values = to_numeric_strict(df[column], column)

    ranges = spec["ranges"]
    # No declared range means every non-null value is acceptable.
    is_valid = _mask_for(values, ranges) if ranges else values.notna()
    is_excluded = _mask_for(values, spec["exclude_ranges"])
    is_valid &= ~is_excluded

    present_before = int((values.notna() & attended).sum())
    out_of_range = int((values.notna() & ~is_valid & attended).sum())

    # Only valid observations may be carried; everything else becomes a gap first.
    seed = values.where(is_valid)
    filled = seed.groupby(df[participant_col]).ffill()

    if max_carry is not None:
        # Distance in rows since the observation being carried, per participant.
        seen = seed.notna()
        block = seen.groupby(df[participant_col]).cumsum()
        distance = seed.groupby([df[participant_col], block]).cumcount()
        filled = filled.where(seen | (distance <= max_carry), np.nan)

    # Never fabricate a visit that did not happen.
    filled = filled.where(attended, np.nan)

    imputed = int((filled.notna() & ~values.where(is_valid).notna() & attended).sum())
    still_missing = int((filled.isna() & attended).sum())

    return filled, {
        "column": column,
        "valid_ranges": json.dumps(ranges) if ranges else "any",
        "values_present": present_before,
        "out_of_range": out_of_range,
        "values_imputed": imputed,
        "still_missing": still_missing,
    }


def invariant_columns(
    df: pd.DataFrame, participant_col: str, protected: set[str]
) -> list[str]:
    """Columns proven constant within every participant, so safe to copy onto a padded row.

    Two conditions, and the second is the one that matters. A column must never take
    two different values for the same participant, AND some participant must have been
    observed in it at least twice — otherwise 'constant' just means 'recorded once and
    blank after', which describes every per-visit measurement in a sparse table. Copying
    one of those onto a visit that never happened would fabricate an observation.
    """
    candidates = []
    for column in df.columns:
        if column in protected:
            continue
        grouped = df.groupby(participant_col)[column]
        if not (grouped.nunique(dropna=True) <= 1).all():
            continue
        if grouped.count().max() < 2:  # never seen twice for anyone: no evidence
            continue
        candidates.append(column)
    return candidates


def add_placeholder_rows(
    df: pd.DataFrame,
    participant_col: str,
    order_col: str,
    timepoint_col: str | None,
    protected: set[str] | None = None,
) -> pd.DataFrame:
    """Pad every participant out to one row per timepoint, blank where absent.

    Participant-level facts (sex, site, cohort) are copied onto the padded rows —
    they are true whether or not the visit happened. Per-visit measurements, the
    attendance column and anything being filled stay blank: `protected` names them.
    """
    indices = pd.to_numeric(df[order_col], errors="coerce").dropna()
    if indices.empty:
        return df
    max_observed = int(indices.max())

    present = {
        (participant, int(index))
        for participant, index in zip(df[participant_col], df[order_col])
        if pd.notna(index)
    }

    protected = set(protected or ()) | {participant_col, order_col}
    if timepoint_col:
        protected.add(timepoint_col)
    static = invariant_columns(df, participant_col, protected)
    if static:
        log(f"Carrying participant-level column(s) onto padded rows: {', '.join(static)}")
    # The first row of a participant may itself be blank in a static column, so take
    # the first value they actually have.
    static_values = {
        column: df.groupby(participant_col)[column].apply(
            lambda s: s.dropna().iloc[0] if s.notna().any() else np.nan
        )
        for column in static
    }

    blanks = []
    for participant in df[participant_col].unique():
        for index in range(1, max_observed + 1):
            if (participant, index) not in present:
                row = {participant_col: participant, order_col: index, "is_placeholder": True}
                if timepoint_col:
                    row[timepoint_col] = f"T{index}"
                for column in static:
                    row[column] = static_values[column].get(participant, np.nan)
                blanks.append(row)

    df = df.copy()
    df["is_placeholder"] = False
    if not blanks:
        return df

    padded = pd.concat([df, pd.DataFrame(blanks)], ignore_index=True)
    return padded.sort_values([participant_col, order_col]).reset_index(drop=True)


def process(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    participant_col = str(config.get("participant_column") or "Barcode").strip()
    order_col = str(config.get("order_column") or "timepoint_index").strip()
    attendance_col = (config.get("attendance_column") or "").strip()
    timepoint_col = (config.get("timepoint_column") or "timepoint").strip()
    emit_grid = bool(config.get("emit_full_grid", False))
    raw_max_carry = config.get("max_carry")
    max_carry = int(raw_max_carry) if raw_max_carry not in (None, "") else None

    parameters = parse_parameters(config.get("parameters"))

    for column in (participant_col, order_col):
        if column not in df.columns:
            raise ValueError(
                f"Column '{column}' not found in input. Available columns: "
                + ", ".join(map(str, df.columns[:40]))
            )

    missing_columns = [c for c in parameters if c not in df.columns]
    if missing_columns:
        raise ValueError(
            "These columns are configured in 'Columns to fill' but are not in the input: "
            + ", ".join(missing_columns)
            + ". Available columns: "
            + ", ".join(map(str, df.columns))
            + ". (The node.json placeholder uses real CBR column names — replace them "
            "with the columns your data actually has.)"
        )

    df = df.copy()
    df[order_col] = pd.to_numeric(df[order_col], errors="coerce")

    if emit_grid:
        before = len(df)
        df = add_placeholder_rows(
            df,
            participant_col,
            order_col,
            timepoint_col if timepoint_col in df.columns else None,
            # A padded row is a visit that did not happen: it must never claim
            # attendance, and never carry a measurement we are about to fill.
            protected={attendance_col, *parameters} - {""},
        )
        log(f"Padded to a full participant x timepoint grid: {before} -> {len(df)} row(s)")
    elif "is_placeholder" not in df.columns:
        df["is_placeholder"] = False

    # LOCF is meaningless in arbitrary row order — sort before filling, not after.
    df = df.sort_values([participant_col, order_col]).reset_index(drop=True)

    if attendance_col:
        if attendance_col not in df.columns:
            raise ValueError(f"Attendance column '{attendance_col}' not found in input")
        attended = df[attendance_col].notna()
    else:
        attended = ~df["is_placeholder"].fillna(False).astype(bool)
    log(f"{int(attended.sum())} of {len(df)} row(s) are attended visits")

    report_rows = []
    for column, spec in parameters.items():
        filled, report = fill_column(
            df, column, spec, participant_col, attended, max_carry
        )
        df[column] = filled
        report_rows.append(report)
        log(
            f"{column}: {report['values_imputed']} imputed, "
            f"{report['out_of_range']} out of range, {report['still_missing']} still missing"
        )

    report = pd.DataFrame(
        report_rows,
        columns=[
            "column",
            "valid_ranges",
            "values_present",
            "out_of_range",
            "values_imputed",
            "still_missing",
        ],
    )
    return df, report


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    ctx = json.loads(os.environ["NODE_CONTEXT"])
    node_name = ctx["node"]["name"]
    config = ctx.get("config", {})
    inputs = ctx.get("inputs", [])
    out_files = ctx["output"]["files"]

    if not inputs:
        raise ValueError("This node requires one upstream input file")
    if len(inputs) > 1:
        log(f"{len(inputs)} inputs connected — using the first ('{inputs[0]['nodeName']}')")

    workdir = tempfile.mkdtemp(prefix="locf_")
    try:
        source = inputs[0]["output"]["path"]
        bucket, key = split_s3_uri(source)
        local_path = os.path.join(workdir, os.path.basename(key))
        log(f"Downloading s3://{bucket}/{key}")
        get_s3_client_for_path(source).download_file(bucket, key, local_path)

        df = read_table(local_path)
        log(f"Read {len(df)} row(s), {len(df.columns)} column(s)")

        filled, report = process(df, config)

        by_name = {out.get("name"): out for out in out_files}
        outputs = []
        for name, frame in (("filled", filled), ("fill_report", report)):
            out = by_name.get(name)
            if out is None:
                log(f"No output file declared for '{name}' — skipping")
                continue
            local_out = os.path.join(workdir, f"{name}.parquet")
            frame.to_parquet(local_out, index=False)
            out_bucket, out_key = split_s3_uri(out["path"])
            log(f"Uploading {name} ({len(frame)} rows) to s3://{out_bucket}/{out_key}")
            get_s3_client_for_path(out["path"]).upload_file(local_out, out_bucket, out_key)
            outputs.append({"name": name, "path": out["path"]})

        print(json.dumps({"success": True, "nodeName": node_name, "outputs": outputs}))
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log_error(str(exc))
        print(json.dumps({"success": False, "error": str(exc)}))
        sys.exit(1)
