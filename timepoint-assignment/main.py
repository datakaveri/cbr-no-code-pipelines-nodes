"""Visit Timepoint Assignment node.

Converts a longitudinal visit table into a timepointed one. For each participant
the earliest visit is the baseline; every visit's elapsed time from that baseline
is matched against the cohort's timepoint window schedule.

Contract:
- Reads NODE_CONTEXT (JSON env var) for node identity, inputs, outputs, config.
- Reads INPUT_S3_* / ARTIFACT_S3_* for storage. Input files may live in either
  bucket; outputs always go to the artifact bucket.
- Writes to the EXACT paths in output.files[i].path.
- Prints a final JSON status to stdout; exits 0 on success, 1 on failure.
"""

import json
import os
import shutil
import sys
import tempfile

import storage_boto3 as boto3
import pandas as pd
import requests
from storage_v2 import read_to_file

NODE_PREFIX = "[TIMEPOINT ASSIGNMENT]"


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
    """Read a local tabular file, dispatching on extension."""
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
# Timepoint windows
# ---------------------------------------------------------------------------

# Each schedule is expressed as:
#   T1              -> [0, t1_max]
#   T2              -> [t2_min, t2_max]
#   Tn for n >= 3   -> [(n-1)*interval - lower_margin, (n-1)*interval + upper_margin]
#
# These reproduce the CBR protocol windows exactly:
#   TLSA    annual:   T1 <= 0.74, T2 = [0.75, 1.50], Tn = [n-1.49, n-0.50]
#   SANSCOG biennial: T1 <= 1.49, T2 = [1.50, 3.00], Tn = [2n-2.9, 2n-1.0]
SCHEMES = {
    "TLSA": {
        "interval_years": 1.0,
        "t1_max": 0.74,
        "t2_min": 0.75,
        "t2_max": 1.50,
        "lower_margin": 0.49,
        "upper_margin": 0.50,
    },
    "SANSCOG": {
        "interval_years": 2.0,
        "t1_max": 1.49,
        "t2_min": 1.50,
        "t2_max": 3.00,
        "lower_margin": 0.90,
        "upper_margin": 1.00,
    },
}

REQUIRED_WINDOW_KEYS = (
    "interval_years",
    "t1_max",
    "t2_min",
    "t2_max",
    "lower_margin",
    "upper_margin",
)


def resolve_windows(scheme: str, custom: object) -> dict:
    if scheme != "custom":
        if scheme not in SCHEMES:
            raise ValueError(f"Unknown timepoint schedule '{scheme}'")
        return dict(SCHEMES[scheme])

    if isinstance(custom, str):
        custom = json.loads(custom) if custom.strip() else {}
    if not isinstance(custom, dict) or not custom:
        raise ValueError(
            "Timepoint schedule is 'custom' but no custom window definition was provided"
        )
    # Explicit form: one window per timepoint, in order. Needed for schedules that
    # are not a repeating interval — a trial sampling at day 0, 7, 30, 90 and 180
    # cannot be expressed as (n-1) x interval no matter what margins you pick.
    if "windows" in custom:
        raw = custom["windows"]
        if not isinstance(raw, (list, tuple)) or not raw:
            raise ValueError("custom_windows.windows must be a non-empty list of [min, max] pairs")
        explicit = []
        for position, item in enumerate(raw, start=1):
            if not isinstance(item, (list, tuple)) or len(item) != 2:
                raise ValueError(
                    f"custom_windows.windows[{position - 1}] must be a [min, max] pair, got: {item!r}"
                )
            low, high = float(item[0]), float(item[1])
            if low > high:
                raise ValueError(f"custom_windows.windows[{position - 1}]: {low} is greater than {high}")
            explicit.append((low, high))
        return {"explicit": explicit, "interval_years": None}

    missing = [k for k in REQUIRED_WINDOW_KEYS if k not in custom]
    if missing:
        raise ValueError(
            f"Custom window definition is missing keys: {', '.join(missing)}. "
            "Provide either those keys for a repeating interval, or a 'windows' "
            "list of [min, max] pairs for an irregular schedule."
        )
    windows = {k: float(custom[k]) for k in REQUIRED_WINDOW_KEYS}
    if windows["interval_years"] <= 0:
        raise ValueError("interval_years must be greater than 0")
    if windows["t2_min"] > windows["t2_max"]:
        raise ValueError("t2_min cannot be greater than t2_max")
    return windows


def timepoint_count(w: dict, max_tp: int) -> int:
    """How many timepoints this schedule defines, bounded by the configured maximum."""
    explicit = w.get("explicit")
    return min(len(explicit), max_tp) if explicit else max_tp


def window_bounds(tp: int, w: dict) -> tuple[float, float]:
    explicit = w.get("explicit")
    if explicit:
        return explicit[tp - 1]
    if tp == 1:
        return 0.0, w["t1_max"]
    if tp == 2:
        return w["t2_min"], w["t2_max"]
    centre = (tp - 1) * w["interval_years"]
    return centre - w["lower_margin"], centre + w["upper_margin"]


def ideal_years(tp: int, w: dict) -> float:
    """The scheduled time for a timepoint: T1 at 0, T2 at one interval, etc.

    This is when the protocol asks the subject to attend, which is NOT the centre
    of the assignment window — the windows are widened to be contiguous so a late
    visit still lands somewhere instead of being discarded. For an explicit
    schedule there is no separate target, so the window's midpoint is used.
    """
    if w.get("explicit"):
        low, high = window_bounds(tp, w)
        return (low + high) / 2.0
    return (tp - 1) * w["interval_years"]


def window_centre(tp: int, w: dict) -> float:
    if w.get("explicit"):
        low, high = window_bounds(tp, w)
        return (low + high) / 2.0
    if tp == 1:
        return 0.0
    if tp == 2:
        return (w["t2_min"] + w["t2_max"]) / 2.0
    return (tp - 1) * w["interval_years"]


def assign_timepoint(elapsed: float, w: dict, max_tp: int) -> int | None:
    """First timepoint whose window contains `elapsed`, or None if it falls in a gap.

    A single function serves both assignment and duplicate detection, so the two
    can never disagree about which side of a boundary a visit sits on.
    """
    for tp in range(1, timepoint_count(w, max_tp) + 1):
        low, high = window_bounds(tp, w)
        if low <= elapsed <= high:
            return tp
    return None


# ---------------------------------------------------------------------------
# Assignment
# ---------------------------------------------------------------------------


def choose(candidates: list[int], years: pd.Series, tp: int, rule: str, w: dict) -> int:
    """Pick the surviving row index for one participant/timepoint collision."""
    if len(candidates) == 1:
        return candidates[0]
    if rule == "earliest":
        return min(candidates, key=lambda i: years.iloc[i])
    if rule == "closest":
        centre = window_centre(tp, w)
        return min(candidates, key=lambda i: (abs(years.iloc[i] - centre), years.iloc[i]))
    return max(candidates, key=lambda i: years.iloc[i])  # "latest" (default)


def assign_for_participant(
    group: pd.DataFrame,
    date_col: str,
    w: dict,
    max_tp: int,
    rule: str,
    days_per_year: float,
    promote: bool,
    promote_fraction: float,
) -> tuple[pd.Series, pd.Series, pd.Series, list[dict]]:
    """Return (years, timepoint, selected, collisions) aligned to `group`'s index."""
    baseline = group[date_col].min()
    years = ((group[date_col] - baseline).dt.days / days_per_year).round(2)

    # Positional index -> candidate timepoint
    candidates: dict[int, list[int]] = {}
    tp_of_row: list[int | None] = []
    for pos in range(len(group)):
        tp = assign_timepoint(float(years.iloc[pos]), w, max_tp)
        tp_of_row.append(tp)
        if tp is not None:
            candidates.setdefault(tp, []).append(pos)

    # Resolve each window to a single surviving visit. T1 is exempt from the
    # collision rule: the baseline is the earliest visit by definition, so a later
    # visit still inside the T1 window can never displace it.
    chosen: dict[int, int] = {}
    for tp, rows in sorted(candidates.items()):
        if tp == 1:
            chosen[tp] = min(rows, key=lambda i: years.iloc[i])
        else:
            chosen[tp] = choose(rows, years, tp, rule, w)

    # Optionally promote a late baseline-window visit into an empty T2 slot.
    # Runs ONCE, after every window is resolved, and never reuses the row that is
    # already serving as T1 — otherwise a participant with a single late visit
    # would have their T1 relabelled and lose the baseline entirely.
    if promote and 2 not in chosen and 1 in candidates:
        t1_max = window_bounds(1, w)[1]
        floor = t1_max * promote_fraction
        eligible = [
            pos
            for pos in candidates[1]
            if pos != chosen.get(1) and floor <= float(years.iloc[pos]) <= t1_max
        ]
        if eligible:
            promoted = max(eligible, key=lambda i: years.iloc[i])
            chosen[2] = promoted
            tp_of_row[promoted] = 2

    selected_positions = set(chosen.values())

    collisions = []
    for tp, rows in sorted(candidates.items()):
        if len(rows) > 1:
            collisions.append(
                {
                    "timepoint": f"T{tp}",
                    "timepoint_index": tp,
                    "n_visits": len(rows),
                    "candidate_years": ", ".join(f"{float(years.iloc[i]):.2f}" for i in rows),
                    "candidate_dates": ", ".join(
                        pd.Timestamp(group[date_col].iloc[i]).date().isoformat() for i in rows
                    ),
                    "kept_years": round(float(years.iloc[chosen[tp]]), 2),
                }
            )

    timepoint = pd.Series(
        [f"T{tp}" if tp is not None else None for tp in tp_of_row], index=group.index
    )
    selected = pd.Series(
        [pos in selected_positions for pos in range(len(group))], index=group.index
    )
    years.index = group.index
    return years, timepoint, selected, collisions


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def process(df: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    participant_col = str(config.get("participant_column") or "Barcode").strip()
    date_col = str(config.get("date_column") or "AppointmentDate").strip()
    cohort_col = (config.get("cohort_column") or "").strip()
    cohort_value = (config.get("cohort_value") or "").strip()
    scheme = str(config.get("scheme") or "TLSA").strip()
    rule = str(config.get("collision_rule") or "latest").strip()
    days_per_year = float(config.get("days_per_year") or 365.25)
    max_tp = int(config.get("max_timepoints") or 20)
    promote = bool(config.get("promote_late_baseline", False))
    promote_fraction = float(config.get("promote_window_fraction") or 0.95)
    drop_unassigned = bool(config.get("drop_unassigned", True))
    tolerance_years = float(config.get("tolerance_years") or 0.25)

    for column in (participant_col, date_col):
        if column not in df.columns:
            raise ValueError(
                f"Column '{column}' not found in input. Available columns: "
                + ", ".join(map(str, df.columns[:40]))
            )

    windows = resolve_windows(scheme, config.get("custom_windows"))
    log(f"Schedule '{scheme}': {windows}")

    if cohort_col:
        if cohort_col not in df.columns:
            raise ValueError(f"Cohort column '{cohort_col}' not found in input")
        if cohort_value:
            before = len(df)
            df = df[df[cohort_col].astype(str).str.strip() == cohort_value].copy()
            log(f"Cohort filter {cohort_col}=='{cohort_value}': {before} -> {len(df)} rows")
        else:
            log(f"Cohort column '{cohort_col}' set but no cohort value given — no filter applied")

    if df.empty:
        raise ValueError("No rows left to process after filtering")

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    undated = int(df[date_col].isna().sum())
    if undated:
        log(f"Dropping {undated} row(s) with a missing or unparseable {date_col}")
        df = df[df[date_col].notna()].copy()
    if df.empty:
        raise ValueError(f"Every row had a missing or unparseable {date_col}")

    df = df.sort_values([participant_col, date_col]).reset_index(drop=True)

    years_parts, tp_parts, selected_parts = [], [], []
    collision_rows = []
    for participant, group in df.groupby(participant_col, sort=False):
        years, timepoint, selected, collisions = assign_for_participant(
            group, date_col, windows, max_tp, rule, days_per_year, promote, promote_fraction
        )
        years_parts.append(years)
        tp_parts.append(timepoint)
        selected_parts.append(selected)
        for collision in collisions:
            collision_rows.append({participant_col: participant, **collision})

    df["years_from_baseline"] = pd.concat(years_parts).reindex(df.index)
    df["timepoint"] = pd.concat(tp_parts).reindex(df.index)
    df["is_selected_visit"] = pd.concat(selected_parts).reindex(df.index)
    df["timepoint_index"] = pd.to_numeric(
        df["timepoint"].str.slice(1), errors="coerce"
    ).astype("Int64")

    # How far each visit was from the anniversary the protocol asked for. Assignment
    # uses the widened contiguous windows so nothing is discarded; this column keeps
    # the protocol's tolerance available, so an analyst can restrict to on-schedule
    # visits later without re-running the pipeline and losing the rest.
    scheduled = df["timepoint_index"].map(
        lambda tp: ideal_years(int(tp), windows) if pd.notna(tp) else pd.NA
    ).astype("Float64")
    df["deviation_years"] = (df["years_from_baseline"] - scheduled).round(2)
    df["within_tolerance"] = (df["deviation_years"].abs() <= tolerance_years).astype("boolean")

    unassigned = int(df["timepoint"].isna().sum())
    superseded = int((~df["is_selected_visit"] & df["timepoint"].notna()).sum())
    log(
        f"Assigned {len(df) - unassigned} visit(s); {unassigned} fell outside every window; "
        f"{superseded} lost a within-window collision"
    )

    if drop_unassigned:
        df = df[df["is_selected_visit"]].copy()
        log(f"Dropped unassigned and superseded visits — {len(df)} row(s) remain")

    # Counted after the drop so the number matches what actually leaves the node.
    off_schedule = int((df["within_tolerance"] == False).sum())  # noqa: E712 — nullable boolean
    log(
        f"{off_schedule} emitted visit(s) fell outside the +/-{tolerance_years}y protocol "
        f"window (kept and flagged via within_tolerance, not dropped)"
    )

    duplicates = pd.DataFrame(
        collision_rows,
        columns=[
            participant_col,
            "timepoint",
            "timepoint_index",
            "n_visits",
            "candidate_years",
            "candidate_dates",
            "kept_years",
        ],
    )
    log(
        f"{len(duplicates)} participant/timepoint collision(s) across "
        f"{duplicates[participant_col].nunique() if len(duplicates) else 0} participant(s)"
    )
    return df, duplicates


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

    workdir = tempfile.mkdtemp(prefix="timepoint_")
    try:
        source_entry = inputs[0]["output"]
        source = source_entry["path"]
        _, key = split_s3_uri(source)
        local_path = os.path.join(workdir, os.path.basename(key))
        # Always re-resolve at read time via storage_v2 (POST /api/node/storage/
        # download-url) instead of trusting output.presignedUrl: that field is
        # signed at workflow-submit time, before an upstream node in this same
        # run has written anything, so for same-run inputs it points at a key
        # that doesn't exist yet and 404s. read_to_file() fetches a fresh URL
        # against the file's CURRENT version, which is race-free.
        log(f"Downloading {source}")
        read_to_file(source, local_path)

        df = read_table(local_path)
        log(f"Read {len(df)} row(s), {len(df.columns)} column(s)")

        timepoints, duplicates = process(df, config)

        by_name = {out.get("name"): out for out in out_files}
        results = {
            "timepoints": timepoints,
            "duplicates": duplicates,
        }

        outputs = []
        for name, frame in results.items():
            out = by_name.get(name)
            if out is None:
                log(f"No output file declared for '{name}' — skipping")
                continue
            local_out = os.path.join(workdir, f"{name}.parquet")
            frame.to_parquet(local_out, index=False)
            out_presigned_url = out.get("presignedUrl")
            if out_presigned_url:
                log(f"Uploading {name} ({len(frame)} rows) to {out['path']} via presigned URL")
                with open(local_out, "rb") as fh:
                    resp = requests.put(out_presigned_url, data=fh, timeout=300)
                resp.raise_for_status()
            else:
                out_bucket, out_key = split_s3_uri(out["path"])
                log(f"Uploading {name} ({len(frame)} rows) to s3://{out_bucket}/{out_key}")
                get_s3_client_for_path(out["path"]).upload_file(local_out, out_bucket, out_key)
            outputs.append({"name": name, "path": out["path"]})

        print(
            json.dumps({"success": True, "nodeName": node_name, "outputs": outputs})
        )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log_error(str(exc))
        print(json.dumps({"success": False, "error": str(exc)}))
        sys.exit(1)
