"""The small harness of the Feast cases: a checker that records named verdicts, the case runner and the JSON report (no values, only names and verdicts)."""

from __future__ import annotations

import contextlib
import json
import sys
import time
import traceback
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest import mock

from scripts.features.publication_reads import REPO_ROOT, ReadError

REPORT_ROOT = REPO_ROOT / "data/e2e/feast_cases"


def fake_module(name: str, **attributes: Any) -> ModuleType:
    """A stand-in module with the given attributes, for ``unittest.mock.patch.dict(sys.modules, ...)``."""
    module = ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    return module


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
    """Run the selected cases (all when none is named); an unexpected exception fails the case, names the innermost project frame and keeps its traceback."""
    names = list(selected) or list(cases)
    unknown = [name for name in names if name not in cases]
    if unknown:
        raise SystemExit(f"unknown cases: {', '.join(unknown)}; known: {', '.join(cases)}")
    results = []
    for name in names:
        function, modes = cases[name]
        checker = Checker()
        started = time.monotonic()
        details = ""
        try:
            function(checker)
        except Exception as error:
            frames = traceback.extract_tb(error.__traceback__)
            own = [frame for frame in frames if "scripts/features/" in frame.filename]
            frame = (own or frames)[-1]
            checker.results.append((f"the case ended with an unexpected {type(error).__name__} at {Path(frame.filename).name}:{frame.lineno}", False))
            # Local evidence only: the cases hold no secrets, and the full traceback is the only way to find the cause of a failure inside an image run.
            details = traceback.format_exc()
            sys.stderr.write(f"--- traceback of case {name}\n{details}")
        failed = [check for check, passed in checker.results if not passed]
        results.append(
            {
                "case": name,
                "failure_modes": modes.split(),
                "verdict": "pass" if checker.results and not failed else "fail",
                "checks": len(checker.results),
                "failed": failed,
                "seconds": round(time.monotonic() - started, 3),
                **({"traceback": details} if details else {}),
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


@contextlib.contextmanager
def fake_redis_modules(calls: list[dict[str, Any]]) -> Iterator[None]:
    """Replace ``redis``, ``redis.retry`` and ``redis.backoff`` by recorders of how a client is built (the real module is only in the image)."""

    def build_client(**options: Any) -> Any:
        calls.append(options)
        return SimpleNamespace(**options)

    modules = {
        "redis": fake_module("redis", Redis=build_client),
        "redis.retry": fake_module("redis.retry", Retry=lambda backoff, retries: SimpleNamespace(backoff=backoff, retries=retries)),
        "redis.backoff": fake_module("redis.backoff", NoBackoff=lambda: SimpleNamespace(kind="no backoff")),
    }
    with mock.patch.dict(sys.modules, modules):
        yield
