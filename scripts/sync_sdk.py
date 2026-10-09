#!/usr/bin/env python3
"""Keep every node's vendored copy of pipeline_sdk/ identical to the master.

A node's Docker build context is its own directory, so each node carries its
own copy of the SDK. The master lives at the repo root (./pipeline_sdk); never
edit a node's copy by hand.

    python3 scripts/sync_sdk.py          # copy the master into every node
    python3 scripts/sync_sdk.py --check  # exit 1 if any copy has drifted (CI)

A node is any top-level directory whose Dockerfile contains "pipeline_sdk".
"""

from __future__ import annotations

import filecmp
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MASTER = ROOT / "pipeline_sdk"


def sdk_files() -> list[str]:
    return sorted(p.name for p in MASTER.glob("*.py"))


def nodes() -> list[Path]:
    return sorted(
        d for d in ROOT.iterdir()
        if d.is_dir() and (d / "Dockerfile").is_file() and "pipeline_sdk" in (d / "Dockerfile").read_text()
    )


def drift(node: Path) -> list[str]:
    copy = node / "pipeline_sdk"
    names = sdk_files()
    problems = [f"missing {n}" for n in names if not (copy / n).is_file()]
    problems += [f"differs {n}" for n in names if (copy / n).is_file() and not filecmp.cmp(MASTER / n, copy / n, shallow=False)]
    if copy.is_dir():
        problems += [f"extra {p.name}" for p in copy.glob("*.py") if p.name not in names]
    return problems


def main() -> int:
    check = "--check" in sys.argv[1:]
    bad = 0
    for node in nodes():
        problems = drift(node)
        if check:
            for problem in problems:
                print(f"{node.name}/pipeline_sdk: {problem}")
            bad += bool(problems)
            continue
        if problems:
            shutil.rmtree(node / "pipeline_sdk", ignore_errors=True)
            shutil.copytree(MASTER, node / "pipeline_sdk", ignore=shutil.ignore_patterns("__pycache__"))
            print(f"synced {node.name}")
    if check and bad:
        print("\nRun: python3 scripts/sync_sdk.py", file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
