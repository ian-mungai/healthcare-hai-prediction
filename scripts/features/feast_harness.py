"""The small harness of the Feast cases: a checker that records named verdicts, the case runner and the JSON report (no values, only names and verdicts)."""

from __future__ import annotations

import json
import time
import traceback
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.features.publication_reads import REPO_ROOT, ReadError

REPORT_ROOT = REPO_ROOT / "data/e2e/feast_cases"


@dataclass
class Checker:
    """The checks of one case: a name and a verdict each; the names say what a failure means."""

    results: list[tuple[str, bool]] = field(default_factory=list)

    def check(self, name: str, condition: bool) -> None:
        self.results.append((name, bool(condition)))

    def refused(self, name: str, call: Callable[..., object], contains: str, *arguments: Any, **options: Any) -> str:
        """Record whether ``call(*arguments, **options)`` raised a ReadError whose message holds ``contains``; returns the message ('' when none)."""
        try:
            call(*arguments, **options)
        except ReadError as error:
            self.results.append((name, contains in str(error)))
            return str(error)
        except Exception as error:
            self.results.append((f"{name} (raised {type(error).__name__} instead of a refusal)", False))
            return ""
        self.results.append((f"{name} (nothing was refused)", False))
        return ""


def run_cases(cases: dict[str, tuple[Callable[[Checker], None], str]], selected: Sequence[str]) -> dict[str, Any]:
    """Run the selected cases (all when none is named); an unexpected exception fails the case with its type, never its text."""
    names = list(selected) or list(cases)
    unknown = [name for name in names if name not in cases]
    if unknown:
        raise SystemExit(f"unknown cases: {', '.join(unknown)}; known: {', '.join(cases)}")
    results = []
    for name in names:
        function, modes = cases[name]
        checker = Checker()
        started = time.monotonic()
        try:
            function(checker)
        except Exception as error:
            frame = traceback.extract_tb(error.__traceback__)[-1]
            checker.results.append((f"the case ended with an unexpected {type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}", False))
        failed = [check for check, passed in checker.results if not passed]
        results.append(
            {
                "case": name,
                "failure_modes": modes.split(),
                "verdict": "pass" if checker.results and not failed else "fail",
                "checks": len(checker.results),
                "failed": failed,
                "seconds": round(time.monotonic() - started, 3),
            }
        )
    return {"cases": results, "verdict": "pass" if all(item["verdict"] == "pass" for item in results) else "fail"}


def write_report(part: str, report: dict[str, Any], report_dir: Path | None) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    folder = report_dir if report_dir is not None else REPORT_ROOT / stamp
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"report_{part}.json"
    path.write_text(json.dumps({"part": part, "generated_at": stamp, **report}, indent=2, sort_keys=True) + "\n")
    return path
