"""Scan staged changes or outgoing source files with project-pinned Gitleaks."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LOGGER = logging.getLogger(__name__)


def execute(command: list[str], cwd: Path, environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Execute fixed argument arrays without echoing scanner output or source values."""
    return subprocess.run(command, cwd=cwd, env=environment, capture_output=True, text=True, timeout=180, check=False)  # noqa: S603 - fixed local CLI, no shell.


def scan(repository: Path, staged: bool) -> int:
    """Scan the real index or a temporary tree of nonignored repository source files."""
    git = shutil.which("git")
    if not git:
        raise ValueError("Git is required")
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("GITLEAKS_", "AWS_"))}
    with tempfile.TemporaryDirectory(prefix="source_scan_") as directory:
        scratch = Path(directory)
        config = scratch / "gitleaks.toml"
        config.write_text("[extend]\nuseDefault = true\n", encoding="utf-8")
        command = [
            str(ROOT / ".tools/bin/gitleaks"),
            "git" if staged else "dir",
            "--config",
            str(config),
            "--redact=100",
            "--no-banner",
            "--no-color",
            "--ignore-gitleaks-allow",
            "--gitleaks-ignore-path",
            str(scratch),
            "--report-format",
            "json",
            "--report-path",
            str(scratch / "findings.json"),
        ]
        if staged:
            command.extend(["--pre-commit", "--staged", str(repository)])
        else:
            listing = execute([git, "ls-files", "-z", "--cached", "--others", "--exclude-standard"], repository, environment)
            if listing.returncode:
                raise ValueError("Could not enumerate source files")
            tree = scratch / "source"
            tree.mkdir()
            for name in sorted(set(listing.stdout.split("\0")) - {""}):
                relative = Path(name)
                source = repository / relative
                if relative.is_absolute() or ".." in relative.parts or source.is_symlink():
                    raise ValueError("Unsafe source path")
                if source.is_file():
                    target = tree / relative
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(source, target)
            command.append(str(tree))
        result = execute(command, repository, environment)
        report = scratch / "findings.json"
        findings = json.loads(report.read_text(encoding="utf-8")) if report.exists() else None
        if result.returncode not in {0, 1} or not isinstance(findings, list):
            raise ValueError("Scanner did not complete")
        # Deliberately omit paths, lines and source fragments from logs.
        LOGGER.info("event=secret_scan status=%s findings=%s", "passed" if result.returncode == 0 else "failed", len(findings))
        return result.returncode


def main() -> None:
    """Fail closed on missing tools and scanner errors; never expose matching text."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path.cwd())
    parser.add_argument("--staged", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        status = scan(args.repository.resolve(), args.staged)
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        LOGGER.error("event=secret_scan status=blocked error_type=%s", type(error).__name__)
        raise SystemExit(2) from None
    raise SystemExit(status)


if __name__ == "__main__":
    main()
