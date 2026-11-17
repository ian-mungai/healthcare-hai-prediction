"""``scripts/install.sh`` installs a checkout in order, stops at the first failed step and names it, and a rerun changes nothing.

Every case copies the script into a new fake checkout and runs it with stand-ins first on ``PATH``: ``python3.12`` creates a stand-in ``.venv/bin/python``,
and that stand-in records each call and fails when a case asks for it. The real installers, the network and the real ``.env`` are never touched.
"""

from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

from scripts.process import CompletedProcess, run_command
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "install.sh"
PYTHON_STAND_IN = """#!/bin/bash
printf '%s\\n' "$*" >> "$STAND_IN_LOG"
case "$*" in *"$FAIL_ON"*) [ -n "$FAIL_ON" ] && { echo "stand-in failure" >&2; exit 1; } ;; esac
exit 0
"""
PYTHON312_STAND_IN = """#!/bin/bash
if [ "$1" = "--version" ]; then echo "Python 3.12.7"; exit 0; fi
printf 'python3.12 %s\\n' "$*" >> "$STAND_IN_LOG"
if [ "$1 $2" = "-m venv" ]; then
  mkdir -p "$3/bin"
  cp "$STAND_IN_PYTHON" "$3/bin/python"
  chmod 755 "$3/bin/python"
fi
exit 0
"""
TERRAFORM_STAND_IN = '#!/bin/bash\nprintf "Terraform v%s\\non darwin_arm64\\n" "$TERRAFORM_VERSION"\n'
NODE_STAND_IN = '#!/bin/bash\nprintf "v%s\\n" "$NODE_VERSION"\n'
PLAIN_STAND_IN = "#!/bin/bash\nexit 0\n"
EXPECTED_STEPS = (
    "scripts/quality/install_tools.py",
    "-m scripts.quality.lock_env export host --output",
    "-m pip install --no-cache-dir --require-hashes --only-binary=:all: -r",
    "-m pip check",
    "-m scripts.quality.install_markdownlint",
    "-m scripts.quality.install_sqlfluff",
    "-m scripts.quality.install_dvc",
    "-m scripts.quality.install_lint_tools",
    "-m scripts.lakehouse.dbt_package_archive verify",
    "-m pre_commit install",
    "-m scripts.quality.pre_push install-shim",
)


class Checkout:
    """A fake checkout with the install script, an ``.env.example`` and a folder of stand-in tools."""

    def __init__(self, folder: Path) -> None:
        self.root = folder / "checkout"
        (self.root / "scripts").mkdir(parents=True)
        shutil.copy(SCRIPT, self.root / "scripts" / "install.sh")
        (self.root / ".env.example").write_text("AWS_REGION=your-aws-region\n")
        self.bin = folder / "bin"
        self.bin.mkdir()
        self.log = folder / "calls.log"
        self.temporary = folder / "tmp"
        self.temporary.mkdir()
        for name, text in {"python3.12": PYTHON312_STAND_IN, "terraform": TERRAFORM_STAND_IN, "node": NODE_STAND_IN}.items():
            self.tool(name, text)
        for name in ("git", "aws"):
            self.tool(name, PLAIN_STAND_IN)
        self.docker_app = folder / "Docker.app"
        self.docker_app.mkdir()
        (folder / "python_stand_in").write_text(PYTHON_STAND_IN)
        self.python_stand_in = folder / "python_stand_in"

    def tool(self, name: str, text: str) -> None:
        (self.bin / name).write_text(text)
        (self.bin / name).chmod(0o755)

    def remove_tool(self, name: str) -> None:
        (self.bin / name).unlink()

    def run(self, fail_on: str = "", terraform: str = "1.16.1", node: str = "22.3.0") -> CompletedProcess[str]:
        environment = {
            "PATH": f"{self.bin}{os.pathsep}/usr/bin{os.pathsep}/bin",
            "TMPDIR": str(self.temporary),
            "STAND_IN_LOG": str(self.log),
            "STAND_IN_PYTHON": str(self.python_stand_in),
            "FAIL_ON": fail_on,
            "TERRAFORM_VERSION": terraform,
            "NODE_VERSION": node,
            "HAI_DOCKER_APP": str(self.docker_app),
        }
        return run_command("/bin/bash", ["scripts/install.sh"], cwd=self.root, env=environment, timeout=60)

    def calls(self) -> list[str]:
        return self.log.read_text().splitlines() if self.log.exists() else []


def test_a_first_install_runs_every_step_in_order_and_makes_the_env_file(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    result = checkout.run()
    check(result.returncode == 0, f"a first install must succeed: {result.returncode}\n{result.stdout}\n{result.stderr}")
    calls = checkout.calls()
    check(calls[0].startswith("python3.12 -m venv .venv"), f"the first call must create the environment: {calls[:1]}")
    position = 0
    for expected in EXPECTED_STEPS:
        found = next((index for index, call in enumerate(calls) if index >= position and expected in call), None)
        check(found is not None, f"the step {expected!r} is missing or out of order in {calls}")
        position = (found or 0) + 1
    check((checkout.root / ".env").read_text() == "AWS_REGION=your-aws-region\n", "the script must copy .env.example to .env when .env is absent")
    check(not any(checkout.temporary.iterdir()), f"the script must remove its temporary folder: {list(checkout.temporary.iterdir())}")


def test_a_rerun_creates_nothing_again_and_never_overwrites_the_env_file(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    check(checkout.run().returncode == 0, "the first install must succeed")
    (checkout.root / ".env").write_text("AWS_REGION=us-west-2\n")
    first = len(checkout.calls())
    result = checkout.run()
    check(result.returncode == 0, f"a rerun must succeed: {result.returncode}\n{result.stdout}\n{result.stderr}")
    check((checkout.root / ".env").read_text() == "AWS_REGION=us-west-2\n", "a rerun must never overwrite an existing .env")
    later = checkout.calls()[first:]
    check(not any(call.startswith("python3.12") for call in later), f"a rerun must not create the environment again: {later}")
    check(not any(checkout.temporary.iterdir()), "a rerun must remove its temporary folder")


def test_a_symbolic_link_named_env_is_not_replaced(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    target = tmp_path / "elsewhere.env"
    target.write_text("AWS_REGION=us-west-2\n")
    (checkout.root / ".env").symlink_to(target)
    check(checkout.run().returncode == 0, "the install must succeed")
    check((checkout.root / ".env").is_symlink() and target.read_text() == "AWS_REGION=us-west-2\n", "an existing .env link must stay as it is")


def test_a_failed_step_stops_the_script_and_is_named(tmp_path: Path) -> None:
    for step, marker, later in (
        ("install_sqlfluff", "scripts.quality.install_sqlfluff", "scripts.quality.install_dvc"),
        ("pip install", "-m pip install", "-m pip check"),
        ("install-shim", "install-shim", ""),
    ):
        folder = tmp_path / re.sub(r"\W", "_", step)
        folder.mkdir()
        checkout = Checkout(folder)
        result = checkout.run(fail_on=marker)
        check(result.returncode == 1, f"a failure in {step} must exit 1: {result.returncode}")
        output = result.stdout + result.stderr
        check(step in output and "FAILED" in output, f"the failure must name {step!r}: {output}")
        check(not later or not any(later in call for call in checkout.calls()), f"no step after {step} may run: {checkout.calls()}")
        check(not any(checkout.temporary.iterdir()), f"{step}: a failed run must remove its temporary folder")


def test_a_failed_step_leaves_the_env_file_alone(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    (checkout.root / ".env").write_text("AWS_REGION=us-west-2\n")
    check(checkout.run(fail_on="install_dvc").returncode == 1, "the failure case must fail")
    check((checkout.root / ".env").read_text() == "AWS_REGION=us-west-2\n", "a failed run must not touch .env")


def test_missing_or_old_prerequisites_are_all_named_before_any_step(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    for name in ("terraform", "aws", "python3.12"):
        checkout.remove_tool(name)
    checkout.docker_app.rmdir()
    result = checkout.run(node="20.1.0")
    check(result.returncode == 1, f"missing prerequisites must exit 1: {result.returncode}")
    output = result.stdout + result.stderr
    for name in ("python3.12", "terraform", "aws", "Docker Desktop", "node"):
        check(name in output, f"{name} must be named as missing: {output}")
    check(checkout.calls() == [], f"no step may run before the prerequisites pass: {checkout.calls()}")
    check(not (checkout.root / ".env").exists(), "no .env may be made before the prerequisites pass")


def test_another_terraform_version_is_refused_and_named(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    result = checkout.run(terraform="1.15.0")
    output = result.stdout + result.stderr
    check(result.returncode == 1 and "terraform 1.16.1" in output, f"an older Terraform must be named: {result.returncode} {output}")
    check(checkout.calls() == [], "no step may run with the wrong Terraform")
