"""Run the pinned linters that sit beside lint_sources: TFLint (AWS ruleset), trivy config, OSV-Scanner, zizmor, import-linter, typos and check-jsonschema.

Run ``.venv/bin/python -m scripts.quality.lint_tools <check> [files]``. Versions come from config/quality_tools.json and are verified before each run;
no tool is installed or updated here. Exceptions live in .linter_allowlist as ``<check> <key> -- <reason>`` (a key is ``path:rule`` or
``path:package==version``); an entry that matches nothing is a finding, so the list cannot rot. Each check prints ``path:line: problem`` and returns 1
when anything is left.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import sys
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.process import TimeoutExpired, run_command
from scripts.quality.lint_sources import native, paths
from scripts.quality.repo_checks import read_allowlist

ROOT = Path(__file__).resolve().parents[2]
CHECKS = ("tflint", "trivy-config", "osv-scanner", "zizmor", "import-linter", "typos", "check-jsonschema")
ALLOWLIST = ".linter_allowlist"
RULE = "docs/quality_checks.md (Linters beside lint_sources)"
TFLINT_CONFIG = ROOT / ".tflint.hcl"
TRIVY_SKIPS = ("--skip-files", "**/*.tfvars.json", "--skip-files", "**/*.tfvars", "--skip-files", "**/terraform.tfstate*", "--skip-dirs", "**/.terraform")
SCHEMA_MAP = "config/quality/json_schemas.json"
REQUIREMENT_FILE = re.compile(r"(?:^|/)requirements[^/]*\.txt$")
CONSTRAINT_FILES = ("services/orchestration/constraints-3.12.txt",)  # A pip constraints file; OSV-Scanner reads it as a requirements file.


@dataclass(frozen=True)
class Issue:
    """One linter finding: the allowlist key, where it is and what it says."""

    key: str
    location: str
    problem: str


def pin(name: str) -> dict[str, Any]:
    return json.loads((ROOT / "config/quality_tools.json").read_text(encoding="utf-8"))["tools"][name]


def program(name: str) -> str:
    """Return the path of a pinned tool after checking that it runs at exactly the pinned version.

    A release binary that ``scripts/quality/install_tools.py`` verified into ``.tools/bin`` wins (the hosted runner has only that one); the pinned
    Homebrew or virtual-environment executable is the local fallback.
    """
    entry = pin(name)
    released = ROOT / ".tools/bin" / name
    path = released if released.is_file() else Path(entry["executable"])
    path = path if path.is_absolute() else ROOT / path
    if not path.is_file():
        install = f"brew install {entry['formula']}" if entry["installer"] == "homebrew" else f".venv/bin/python -m {entry['installer'][:-3].replace('/', '.')}"
        raise ValueError(f"config/quality_tools.json:1: {name} {entry['version']} is not installed at {path}. Ask the owner to run `{install}`.")
    version = run_command(str(path), ["--version"], cwd=ROOT, timeout=30)
    if version.returncode or not re.search(rf"(?<![\d.]){re.escape(entry['version'])}(?![\d.])", version.stdout + version.stderr):
        raise ValueError(f"config/quality_tools.json:1: {name} must be version {entry['version']}, found {(version.stdout + version.stderr).strip()[:80]!r}.")
    return str(path)


def clean_environment(**extra: str) -> dict[str, str]:
    """The caller's environment without AWS settings, so no linter can reach an account."""
    return {**{key: value for key, value in os.environ.items() if not key.startswith("AWS_")}, "AWS_EC2_METADATA_DISABLED": "true", **extra}


def report(check: str, issues: Iterable[Issue], *, stale_check: bool = True) -> int:
    """Print every issue the allowlist does not cover and every allowlist entry that matched nothing; return 1 when any is printed."""
    entries, malformed = read_allowlist(ALLOWLIST, list(CHECKS), RULE, "why this finding stays")
    entries = [(kind, glob) for kind, glob in entries if kind == check]
    used: set[str] = set()
    left = []
    for issue in issues:
        match = next((glob for _, glob in entries if fnmatch.fnmatchcase(issue.key, glob)), None)
        if match is None:
            left.append(issue)
        else:
            used.add(match)
    for issue in left:
        sys.stderr.write(f"{issue.location}: {check}: {issue.problem}. Fix it, or add '{check} {issue.key} -- <why it stays>' to {ALLOWLIST}.\n")
    stale = [glob for _, glob in entries if glob not in used] if stale_check else []
    for glob in stale:
        sys.stderr.write(f"{ALLOWLIST}:1: {check}: entry {glob} matched no finding. Remove it.\n")
    for finding in malformed:
        sys.stderr.write(f"{finding.location}: {finding.problem}. {finding.fix}.\n")
    sys.stdout.write(f"{check}: {len(left)} findings, {len(used)} allowlist entries in use\n")
    return int(bool(left or stale or malformed))


def tracked(*patterns: str) -> list[str]:
    return [name for name in paths(tracked=True) if any(fnmatch.fnmatchcase(name, pattern) for pattern in patterns)]


def tflint() -> int:
    """TFLint with the Terraform recommended preset and the pinned AWS ruleset, one run per folder that holds a tracked .tf file."""
    binary = native("tflint")
    plugin = pin("tflint")["plugins"]["aws"]
    plugins = Path(os.environ.get("TFLINT_PLUGIN_DIR", Path.home() / ".tflint.d/plugins"))
    if not (plugins / plugin["source"] / plugin["version"]).is_dir():
        raise ValueError(f".tflint.hcl:1: the AWS ruleset {plugin['version']} is not installed in {plugins}. Ask the owner to run `tflint --init` once.")
    if f'version = "{plugin["version"]}"' not in TFLINT_CONFIG.read_text(encoding="utf-8"):
        raise ValueError(f".tflint.hcl:1: the aws plugin block must pin version {plugin['version']} as config/quality_tools.json does.")
    issues = []
    for folder in sorted({Path(name).parent.as_posix() for name in tracked("*.tf")}):
        arguments = ["--chdir", folder, "--config", str(TFLINT_CONFIG), "--format", "json", "--force"]
        result = run_command(binary, arguments, cwd=ROOT, env=clean_environment(), timeout=120)
        document = json.loads(result.stdout or "{}")
        if result.returncode or document.get("errors"):
            raise ValueError(f"{folder}:1: tflint failed ({result.returncode}): {document.get('errors') or result.stderr[-1500:]}")
        for item in document.get("issues", []):
            where = item["range"]  # tflint reports paths from the starting folder, not from the --chdir folder
            issues.append(
                Issue(
                    f"{where['filename']}:{item['rule']['name']}",
                    f"{where['filename']}:{where['start']['line']}",
                    f"{item['message']} ({item['rule']['name']})",
                )
            )
    return report("tflint", issues)


def trivy_config() -> int:
    """``trivy config`` on infra/ with its embedded checks; variable files and state are skipped by name and never read."""
    binary = native("trivy")
    with tempfile.TemporaryDirectory(prefix="hai_trivy_") as cache:
        arguments = ["config", "infra", "--skip-check-update", "--cache-dir", cache, *TRIVY_SKIPS, "--quiet", "--format", "json"]
        result = run_command(binary, arguments, cwd=ROOT, env=clean_environment(), timeout=180)
    if result.returncode:
        raise ValueError(f"infra:1: trivy config failed ({result.returncode}): {result.stderr[-1500:]}")
    issues = []
    for entry in json.loads(result.stdout).get("Results", []):
        for item in entry.get("Misconfigurations") or []:
            path = f"infra/{entry['Target']}"
            line = item.get("CauseMetadata", {}).get("StartLine", 1)
            issues.append(Issue(f"{path}:{item['ID']}", f"{path}:{line}", f"{item['Title']} ({item['ID']}): {item['Message']}"))
    return report("trivy-config", issues)


def osv_scanner() -> int:
    """OSV-Scanner on the tracked requirements files and the orchestration constraints file; it queries osv.dev, so it needs the network."""
    binary = program("osv-scanner")
    files = [name for name in paths(tracked=True) if REQUIREMENT_FILE.search(name)]
    arguments = ["scan", "source", "--no-resolve", "--format", "json"]
    for name in files:
        arguments += ["-L", name]
    for name in CONSTRAINT_FILES:
        arguments += ["-L", f"requirements.txt:{name}"]
    with tempfile.TemporaryDirectory(prefix="hai_osv_") as folder:
        output = Path(folder) / "report.json"
        result = run_command(binary, [*arguments, "--output-file", str(output)], cwd=ROOT, timeout=300)
        if result.returncode not in {0, 1} or not output.is_file():
            raise ValueError(f"requirements.txt:1: osv-scanner failed ({result.returncode}); it needs network access to osv.dev: {result.stderr[-1500:]}")
        document = json.loads(output.read_text(encoding="utf-8"))
    issues = []
    for entry in document.get("results", []):
        source = Path(entry["source"]["path"]).resolve().relative_to(ROOT).as_posix()
        for item in entry["packages"]:
            package = item["package"]
            ids = sorted({vulnerability["id"] for vulnerability in item["vulnerabilities"]})
            issues.append(
                Issue(
                    f"{source}:{package['name']}=={package['version']}",
                    f"{source}:1",
                    f"{package['name']} {package['version']} has {len(ids)} known vulnerabilities ({ids[0]} and others)",
                )
            )
    sys.stdout.write(f"osv-scanner: {len(files) + len(CONSTRAINT_FILES)} files scanned\n")
    return report("osv-scanner", issues)


def zizmor() -> int:
    """zizmor in offline mode on .github/workflows/ (the online audits need a GitHub token)."""
    result = run_command(program("zizmor"), ["--offline", "--no-progress", "--format", "json", ".github/workflows/"], cwd=ROOT, timeout=120)
    if not result.stdout.strip().startswith("["):
        raise ValueError(f".github/workflows:1: zizmor failed ({result.returncode}): {result.stderr[-1500:]}")
    issues = []
    for item in json.loads(result.stdout):
        primary = next(place for place in item["locations"] if place["symbolic"]["kind"] == "Primary")
        path, row = primary["symbolic"]["key"]["Local"]["verbatim_path"], primary["concrete"]["location"]["start_point"]["row"] + 1
        issues.append(
            Issue(
                f"{path}:{item['ident']}",
                f"{path}:{row}",
                f"{item['desc']} ({item['ident']}, {item['determinations']['severity'].lower()} severity, {item['url']})",
            )
        )
    return report("zizmor", issues)


def import_linter() -> int:
    """import-linter with the contracts in .importlinter, which state the package boundaries that hold today."""
    environment = {**os.environ, "PYTHONPATH": str(ROOT)}
    result = run_command(program("import-linter"), ["--config", ".importlinter", "--no-cache"], cwd=ROOT, env=environment, timeout=300)
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return int(bool(result.returncode))


def typos(files: list[str]) -> int:
    """typos on the given files, or on every tracked text source; _typos.toml holds the excluded data and the accepted domain words."""
    selected = files or paths(tracked=True)
    result = run_command(program("typos"), ["--config", "_typos.toml", "--force-exclude", "--format", "brief", "--", *selected], cwd=ROOT, timeout=300)
    sys.stdout.write(result.stdout)
    sys.stderr.write(result.stderr)
    return int(bool(result.returncode))


def expand(patterns: list[str], files: list[str]) -> list[str]:
    selected = sorted({name for pattern in patterns for name in files if fnmatch.fnmatchcase(name, pattern)})
    if not selected:
        raise ValueError(f"{SCHEMA_MAP}:1: no tracked file matches {patterns}. Update the map.")
    return selected


def check_jsonschema() -> int:
    """check-jsonschema on the mapped JSON contracts, the workflows and actions (bundled schemas) and every *.schema.json against its metaschema."""
    binary = program("check-jsonschema")
    mapping = json.loads((ROOT / SCHEMA_MAP).read_text(encoding="utf-8"))
    files = paths(tracked=True)
    runs: list[tuple[list[str], list[str]]] = [(["--check-metaschema"], expand(["*.schema.json"], files))]
    runs += [(["--builtin-schema", row["schema"]], expand(row["files"], files)) for row in mapping["builtin"]]
    runs += [(["--schemafile", row["schema"]], expand(row["files"], files)) for row in mapping["pairs"]]
    failed = False
    for arguments, selected in runs:
        result = run_command(binary, ["--no-cache", *arguments, "--", *selected], cwd=ROOT, timeout=120)
        failed |= bool(result.returncode)
        if result.returncode:
            sys.stderr.write(f"{' '.join(arguments)}: {len(selected)} files\n{result.stdout}{result.stderr}")
    sys.stdout.write(f"check-jsonschema: {len(runs)} runs, {sum(len(selected) for _, selected in runs)} file checks\n")
    return int(failed)


def main() -> int:
    """Run the requested linter and return failure for any finding or any tool that cannot run."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("check", choices=CHECKS)
    parser.add_argument("files", nargs="*", help="typos only: the files to check (default: every tracked source)")
    args = parser.parse_args()
    try:
        runners = {
            "tflint": tflint,
            "trivy-config": trivy_config,
            "osv-scanner": osv_scanner,
            "zizmor": zizmor,
            "import-linter": import_linter,
            "check-jsonschema": check_jsonschema,
        }
        return typos(args.files) if args.check == "typos" else runners[args.check]()
    except (OSError, ValueError, KeyError, TimeoutExpired) as error:
        sys.stderr.write(f"{args.check}: {error}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
