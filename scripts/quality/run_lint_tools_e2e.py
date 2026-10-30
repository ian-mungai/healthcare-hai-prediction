"""E2E for the lint_tools hooks: each real hook passes the project as it is and rejects a planted defect with its own diagnostic.

Run ``.venv/bin/python -m scripts.quality.run_lint_tools_e2e [--tool NAME] [--offline]``. The run copies the working tree (every file Git does not
ignore) into a scratch Git repository, links ``.tools`` and ``.venv``, and starts each hook through ``pre-commit`` itself, so a hook that is not
wired in cannot pass. Per tool it runs the clean copy, plants one defect, then removes it again. Extra cases plant a stale allowlist entry and a
pin that differs from the installed version. ``--offline`` skips the OSV-Scanner case, which queries osv.dev. Logs and ``report.json`` go to
``data/e2e/lint_tools/<UTC>/``.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from scripts.process import clear_git_environment, run_command

ROOT = Path(__file__).resolve().parents[2]
BUCKET = 'resource "aws_s3_bucket" "probe" {\n  bucket = "probe-bucket"\n}\n'
INSTANCE = 'resource "aws_instance" "probe" {\n  ami           = "ami-0abcdef1234567890"\n  instance_type = "t1.2xlarge"\n}\n'
UNPINNED_WORKFLOW = "on: pull_request_target\njobs:\n  probe:\n    runs-on: ubuntu-24.04\n    steps:\n      - uses: actions/checkout@v4\n"
BAD_WORKFLOW = "on: push\njobs:\n  probe:\n    runs-on: ubuntu-24.04\n    not_a_job_key: true\n    steps:\n      - run: echo ok\n"
MISSPELLED = "rec" + "ieve"  # Assembled at run time so this file does not trip the check it tests.
STAGES = {"osv-scanner": "pre-push"}


@dataclass(frozen=True)
class Defect:
    """One planted defect: the hook it must trip, the files to add and the text its output must hold."""

    name: str
    hook: str
    files: dict[str, str]
    diagnostic: str


DEFECTS = (
    Defect("tflint_aws_rule", "tflint", {"infra/zz_probe/main.tf": INSTANCE}, "aws_instance_invalid_type"),
    Defect("trivy_config_bucket", "trivy-config", {"infra/zz_probe/bucket.tf": BUCKET}, "trivy-config:"),
    Defect("osv_scanner_old_pin", "osv-scanner", {"zz_probe/requirements.txt": "requests==2.19.0\n"}, "requests 2.19.0 has"),
    Defect("zizmor_unpinned_action", "zizmor", {".github/workflows/zz_probe.yml": UNPINNED_WORKFLOW}, "zizmor:"),
    Defect("import_linter_upward_import", "import-linter", {"scripts/acquisition/zz_probe.py": "from scripts.lakehouse import silver_quality\n"}, "BROKEN"),
    Defect("typos_misspelling", "typos", {"docs/zz_probe.md": f"We {MISSPELLED} the file.\n"}, MISSPELLED),
    Defect("check_jsonschema_workflow", "check-jsonschema", {".github/workflows/zz_probe.yml": BAD_WORKFLOW}, "not_a_job_key"),
)


def git(repo: Path, *arguments: str) -> str:
    return run_command("git", ["-C", str(repo), *arguments], timeout=120, check=True).stdout


def build_repository(base: Path) -> Path:
    """Copy the files of the working tree that Git does not ignore into a new Git repository and link the installed tools."""
    repo = base / "repo"
    repo.mkdir()
    listed = run_command("git", ["ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT, timeout=60, check=True).stdout
    for name in sorted(set(listed.split("\0")) - {""}):
        target = repo / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, target, follow_symlinks=False)
    git(repo, "init", "--quiet")
    for key, value in (("user.name", "lint tools e2e"), ("user.email", "lint-tools-e2e@example.invalid")):
        git(repo, "config", key, value)
    git(repo, "add", "--all")
    git(repo, "commit", "--quiet", "--no-verify", "--message", "chore: scratch copy")
    (repo / ".git/info/exclude").write_text(".tools\n.venv\n", encoding="utf-8")  # ignored, so `git clean` in restore() keeps the links
    for link in (".tools", ".venv"):
        (repo / link).symlink_to(ROOT / link)
    return repo


def run_hook(repo: Path, hook: str, log: Path) -> tuple[int, str]:
    environment = {key: value for key, value in os.environ.items() if not key.startswith("AWS_")}
    arguments = ["run", hook, "--hook-stage", STAGES.get(hook, "pre-commit"), "--all-files", "--verbose"]
    environment["PRE_COMMIT_HOME"] = str(ROOT / ".tools/lint_caches/pre_commit")
    result = run_command(str(ROOT / ".venv/bin/pre-commit"), arguments, cwd=repo, env=environment, timeout=360)
    output = result.stdout + result.stderr
    log.write_text(output, encoding="utf-8")
    return result.returncode, output


def plant(repo: Path, files: dict[str, str]) -> None:
    for name, content in files.items():
        (repo / name).parent.mkdir(parents=True, exist_ok=True)
        (repo / name).write_text(content, encoding="utf-8")
    git(repo, "add", "--all")


def restore(repo: Path) -> None:
    git(repo, "reset", "--quiet", "--hard")
    git(repo, "clean", "--quiet", "-fd")


def main() -> int:
    """Run the clean and defect cases and fail on any hook that does not behave."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tool", action="append", help="run only the cases of this hook (repeat to select more)")
    parser.add_argument("--offline", action="store_true", help="skip the OSV-Scanner cases, which need the network")
    args = parser.parse_args()
    clear_git_environment()
    selected = [defect for defect in DEFECTS if (not args.tool or defect.hook in args.tool) and not (args.offline and defect.hook == "osv-scanner")]
    out = ROOT / "data/e2e/lint_tools" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out.mkdir(parents=True)
    rows: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory(prefix="hai_lint_tools_e2e_") as temporary:
        repo = build_repository(Path(temporary))
        for defect in selected:
            clean_code, _ = run_hook(repo, defect.hook, out / f"{defect.name}_clean.log")
            plant(repo, defect.files)
            bad_code, bad_output = run_hook(repo, defect.hook, out / f"{defect.name}_defect.log")
            restore(repo)
            again, _ = run_hook(repo, defect.hook, out / f"{defect.name}_restored.log")
            passed = clean_code == 0 and bad_code != 0 and defect.diagnostic in bad_output and again == 0
            rows.append(
                {
                    "case": defect.name,
                    "hook": defect.hook,
                    "clean": clean_code,
                    "defect": bad_code,
                    "restored": again,
                    "diagnostic": defect.diagnostic,
                    "passed": passed,
                }
            )
        extra = [
            ("stale_allowlist_entry", "zizmor", ".linter_allowlist", "zizmor nothing/here.yml:self-repository -- probe\n", "matched no finding", True),
            ("pin_differs_from_installed", "typos", "config/quality_tools.json", None, "must be version 1.0.0", False),
        ]
        for name, hook, path, addition, diagnostic, append in extra if not args.tool else [row for row in extra if row[1] in args.tool]:
            target = repo / path
            if addition is None:
                tools = json.loads(target.read_text(encoding="utf-8"))
                tools["tools"]["typos"]["version"] = "1.0.0"
                target.write_text(json.dumps(tools, indent=2) + "\n", encoding="utf-8")
            elif append:
                target.write_text(target.read_text(encoding="utf-8") + addition, encoding="utf-8")
            git(repo, "add", "--all")
            code, output = run_hook(repo, hook, out / f"{name}.log")
            restore(repo)
            rows.append({"case": name, "hook": hook, "defect": code, "diagnostic": diagnostic, "passed": code != 0 and diagnostic in output})
    (out / "report.json").write_text(json.dumps({"cases": rows, "passed": all(row["passed"] for row in rows)}, indent=2) + "\n", encoding="utf-8")
    for row in rows:
        sys.stdout.write(f"{'PASS' if row['passed'] else 'FAIL'} {row['case']}\n")
    sys.stdout.write(f"report: {out.relative_to(ROOT)}/report.json\n")
    return int(not all(row["passed"] for row in rows))


if __name__ == "__main__":
    raise SystemExit(main())
