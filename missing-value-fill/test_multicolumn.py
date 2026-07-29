"""Why columns other than the configured ones come back N/A.

A realistic slice: two participants, three timepoints, a mix of column kinds —
static demographics, a per-visit measure that IS configured to fill, and two
per-visit measures that are NOT.

Run: python test_multicolumn.py
"""

import pandas as pd

from main import process

FAILURES = []


def check(label, actual, expected):
    if actual != expected:
        FAILURES.append(f"{label}: expected {expected!r}, got {actual!r}")


COLUMNS = [
    "Barcode",
    "timepoint_index",
    "timepoint",
    "AppointmentDate",
    "Gender",       # static per participant
    "Age",          # per visit, always recorded
    "hmse",         # per visit, CONFIGURED to fill
    "mmse",         # per visit, not configured
    "bp_systolic",  # per visit, not configured
]

ROWS = [
    ("P1", 1, "T1", "2020-01-01", "F", 70, 24, 28, 120),
    ("P1", 2, "T2", "2021-01-01", "F", 71, None, None, None),
    ("P1", 3, "T3", "2022-01-01", "F", 72, 22, None, 130),
    ("P2", 1, "T1", "2020-01-01", "M", 65, 28, 30, 118),
    # P2 has no row at all for T2 — they skipped the visit.
    ("P2", 3, "T3", "2022-01-01", "M", 67, None, None, None),
]

BASE = {
    "participant_column": "Barcode",
    "order_column": "timepoint_index",
    "attendance_column": "AppointmentDate",
    "timepoint_column": "timepoint",
    "emit_full_grid": False,
    "parameters": {"hmse": [[1, 50]]},
}


def frame():
    return pd.DataFrame(ROWS, columns=COLUMNS)


def column(df, name, participant="P1"):
    values = df[df["Barcode"] == participant][name]
    return [None if pd.isna(v) else v for v in values]


def na_counts(df):
    return {c: int(df[c].isna().sum()) for c in df.columns if c != "is_placeholder"}


# ---------------------------------------------------------------------------
# Cause 1: only the columns listed in `parameters` are ever filled
# ---------------------------------------------------------------------------


def test_configured_column_is_filled():
    out, _ = process(frame(), dict(BASE))
    check("hmse carried forward for P1", column(out, "hmse"), [24.0, 24.0, 22.0])
    check("hmse carried forward for P2", column(out, "hmse", "P2"), [28.0, 28.0])


def test_unconfigured_columns_keep_every_gap():
    """mmse and bp_systolic are not in `parameters`, so LOCF never touches them."""
    out, _ = process(frame(), dict(BASE))
    check("mmse untouched", column(out, "mmse"), [28.0, None, None])
    check("bp_systolic untouched", column(out, "bp_systolic"), [120.0, None, 130.0])
    check("P2 mmse untouched", column(out, "mmse", "P2"), [30.0, None])


def test_listing_a_column_with_no_range_fills_it():
    """`"mmse": []` means 'fill this, accept any non-null value' — the fix."""
    config = dict(BASE, parameters={"hmse": [[1, 50]], "mmse": [], "bp_systolic": []})
    out, report = process(frame(), config)
    check("mmse now filled", column(out, "mmse"), [28.0, 28.0, 28.0])
    check("bp_systolic now filled", column(out, "bp_systolic"), [120.0, 120.0, 130.0])
    check("all three columns reported", len(report), 3)
    check(
        "no-range columns reported as 'any'",
        list(report[report["column"] == "mmse"]["valid_ranges"]),
        ["any"],
    )


def test_string_column_is_rejected_not_blanked():
    """Gender is text. Coercing it would silently blank every value, so refuse."""
    try:
        process(frame(), dict(BASE, parameters={"hmse": [[1, 50]], "Gender": []}))
        FAILURES.append("text column: expected a ValueError, got none")
    except ValueError as exc:
        check("names the column", "Gender" in str(exc), True)
        check("shows an offending value", "'F'" in str(exc), True)


def test_blank_strings_still_count_as_missing():
    """Empty cells in a text-typed numeric column are missing data, not a type error."""
    df = frame()
    df["hmse"] = df["hmse"].astype(object)
    df.loc[1, "hmse"] = "   "
    out, _ = process(df, dict(BASE))
    check("whitespace treated as a gap and filled", column(out, "hmse"), [24.0, 24.0, 22.0])


# ---------------------------------------------------------------------------
# Cause 2: full-grid padding invents rows that are blank in EVERY column
# ---------------------------------------------------------------------------


def test_grid_is_off_by_default():
    """Padding adds blank cells, so a fill node must not do it unasked."""
    config = dict(BASE)
    del config["emit_full_grid"]
    out, _ = process(frame(), config)
    check("no rows invented", len(out), 5)


def test_padded_rows_keep_participant_level_facts():
    out, _ = process(frame(), dict(BASE, emit_full_grid=True))
    check("grid is rectangular", len(out), 6)

    padded = out[out["is_placeholder"]]
    check("one padded row", len(padded), 1)

    row = padded.iloc[0]
    check("padded row is P2 at T2", (row["Barcode"], int(row["timepoint_index"])), ("P2", 2))
    check("timepoint label written", row["timepoint"], "T2")
    # Gender is constant within every participant, and P1 has 3 rows proving it.
    check("static Gender carried onto the padded row", row["Gender"], "M")
    # Age changes between visits, so it is not a participant-level fact.
    check("per-visit Age left blank", pd.isna(row["Age"]), True)
    check("AppointmentDate blank, so the row counts as unattended", pd.isna(row["AppointmentDate"]), True)
    check("configured column not filled either", pd.isna(row["hmse"]), True)
    # mmse is observed once per participant and blank after. That is a sparse
    # measurement, not a participant-level fact — it must not be copied.
    check("sparse measurement not copied", pd.isna(row["mmse"]), True)
    check("bp_systolic not copied", pd.isna(row["bp_systolic"]), True)


def test_padding_never_fabricates_an_observation():
    """With one row each, a constant column is not evidence of invariance."""
    single = pd.DataFrame(
        [
            ("P1", 1, "T1", "2020-01-01", "F", 70, 24, 28, 120),
            ("P2", 2, "T2", "2021-01-01", "M", 65, 28, 30, 118),
        ],
        columns=COLUMNS,
    )
    out, _ = process(single, dict(BASE, emit_full_grid=True))
    padded = out[out["is_placeholder"]]
    check("two padded rows", len(padded), 2)
    check("nothing copied across", int(padded["Gender"].notna().sum()), 0)
    check("no measurement invented", int(padded["mmse"].notna().sum()), 0)


def test_padding_adds_more_nas_than_it_fills():
    without, _ = process(frame(), dict(BASE, emit_full_grid=False))
    with_grid, _ = process(frame(), dict(BASE, emit_full_grid=True))

    before = sum(na_counts(without).values())
    after = sum(na_counts(with_grid).values())
    check("padding strictly increases the number of blank cells", after > before, True)


# ---------------------------------------------------------------------------
# Diagnostic
# ---------------------------------------------------------------------------


def report_na_table():
    raw = frame()
    no_grid, _ = process(frame(), dict(BASE, emit_full_grid=False))
    grid, _ = process(frame(), dict(BASE, emit_full_grid=True))
    everything = dict(BASE, parameters={"hmse": [[1, 50]], "mmse": [], "bp_systolic": []})
    all_cols, _ = process(frame(), everything)

    stages = [
        (f"input ({len(raw)} rows)", na_counts(raw)),
        (f"filled, grid off ({len(no_grid)} rows)", na_counts(no_grid)),
        (f"filled, grid on ({len(grid)} rows)", na_counts(grid)),
        (f"all 3 cols configured ({len(all_cols)} rows)", na_counts(all_cols)),
    ]

    width = max(len(c) for c in COLUMNS) + 2
    print("\nN/A cells per column")
    print("-" * (width + 26 * len(stages)))
    print("column".ljust(width) + "".join(name.ljust(26) for name, _ in stages))
    for col in COLUMNS:
        line = col.ljust(width)
        for _, counts in stages:
            line += str(counts.get(col, "-")).ljust(26)
        print(line)
    print("-" * (width + 26 * len(stages)))
    print("total".ljust(width) + "".join(str(sum(c.values())).ljust(26) for _, c in stages))
    print()


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for test in tests:
        test()
        print(f"  ran {test.__name__}")

    report_na_table()

    if FAILURES:
        print(f"FAILED ({len(FAILURES)}):")
        for failure in FAILURES[:25]:
            print(f"  - {failure}")
        raise SystemExit(1)
    print(f"All {len(tests)} tests passed.")
