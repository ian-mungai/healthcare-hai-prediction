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


def printing(text: str, exit_code: int = 0) -> str:
    """A stand-in tool that prints ``text`` and exits with ``exit_code``."""
    return f"#!/bin/bash\nprintf '%s\\n' '{text}'\nexit {exit_code}\n"


GIT = "git version 2.54.0"
AWS = "aws-cli/2.37.10 Python/3.13.9 Darwin/27.2.0 source/arm64"
PLUGIN = "1.2.707.0"
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
        for name, text in {"git": GIT, "aws": AWS, "session-manager-plugin": PLUGIN}.items():
            self.tool(name, printing(text))
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


PROBLEMS = {
    "no session-manager-plugin": ("session-manager-plugin", None, "session-manager-plugin"),
    "a failing session-manager-plugin": ("session-manager-plugin", printing(PLUGIN, 42), "session-manager-plugin"),
    "AWS CLI v1": ("aws", printing("aws-cli/1.27.0 Python/3.9.6 Darwin/23.0.0 botocore/1.29.0"), "AWS CLI v2"),
    "a failing aws": ("aws", printing(AWS, 42), "aws"),
    "an old Git": ("git", printing("git version 2.53.9"), "Git 2.54"),
    "a failing Git": ("git", printing(GIT, 42), "git"),
    "an unreadable Git": ("git", printing("not a version"), "git"),
    "a failing Terraform": ("terraform", printing("Terraform v1.16.1", 42), "terraform"),
    "a failing Node": ("node", printing("v22.3.0", 42), "node"),
    "an old Python": ("python3.12", printing("Python 3.11.9"), "python3.12"),
    "a failing Python": ("python3.12", printing("Python 3.12.7", 42), "python3.12"),
}


def test_each_prerequisite_problem_is_named_before_any_step(tmp_path: Path) -> None:
    for number, (case, (tool, stand_in, named)) in enumerate(PROBLEMS.items()):
        folder = tmp_path / f"case{number}"
        folder.mkdir()
        checkout = Checkout(folder)
        if stand_in is None:
            checkout.remove_tool(tool)
        else:
            checkout.tool(tool, stand_in)
        result = checkout.run()
        output = result.stdout + result.stderr
        check(result.returncode == 1, f"{case}: it must exit 1: {result.returncode} {output!r}")
        check("missing prerequisites" in output and named in output, f"{case}: the message must name {named!r}: {output!r}")
        check(checkout.calls() == [], f"{case}: no step may run: {checkout.calls()}")
        check(not (checkout.root / ".env").exists() and not any(checkout.temporary.iterdir()), f"{case}: nothing may be created")


def test_several_prerequisite_problems_are_all_named_at_once(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    checkout.remove_tool("session-manager-plugin")
    checkout.tool("git", printing("git version 2.40.0"))
    checkout.tool("aws", printing("aws-cli/1.2.3"))
    checkout.tool("terraform", printing("Terraform v1.16.1", 42))
    result = checkout.run()
    output = result.stdout + result.stderr
    for named in ("session-manager-plugin", "Git 2.54", "AWS CLI v2", "terraform"):
        check(named in output, f"{named!r} must be named in one message: {output!r}")


def test_newer_and_vendor_versions_pass(tmp_path: Path) -> None:
    for number, git in enumerate(("git version 2.54.0", "git version 2.54.1 (Apple Git-155)", "git version 2.100.0", "git version 3.0.0")):
        folder = tmp_path / f"git{number}"
        folder.mkdir()
        checkout = Checkout(folder)
        checkout.tool("git", printing(git))
        result = checkout.run()
        check(result.returncode == 0, f"{git!r} must pass: {result.returncode} {result.stdout + result.stderr}")


def test_the_git_minimum_is_the_one_the_push_gate_requires() -> None:
    gate = re.search(r"^MINIMUM_GIT = \((\d+), (\d+)\)", (ROOT / "scripts" / "quality" / "pre_push.py").read_text(), re.MULTILINE)
    script = re.search(r'^git_minimum="(\d+)\.(\d+)"', SCRIPT.read_text(), re.MULTILINE)
    check(gate is not None and script is not None, "both files must name the Git minimum")
    check(gate is not None and script is not None and gate.groups() == script.groups(), f"the install script and pre_push.py must agree: {script} {gate}")


def test_the_readme_prerequisites_name_what_the_script_checks() -> None:
    line = next(line for line in (ROOT / "README.md").read_text().splitlines() if line.startswith("Prerequisites:"))
    for named in ("Git 2.54 or later", "AWS CLI v2", "Session Manager plugin", "Terraform 1.16.1", "Node.js 22 or later", "Docker Desktop", "Python 3.12"):
        check(named in line, f"the README prerequisites must name {named!r}: {line}")


def test_the_dbt_archive_is_fetched_only_when_its_verification_fails(tmp_path: Path) -> None:
    (tmp_path / "ok").mkdir()
    first = Checkout(tmp_path / "ok")
    check(first.run().returncode == 0, "the first install must succeed")
    check(not any("dbt_package_archive fetch" in call for call in first.calls()), "a verified archive must not be fetched again")

    folder = tmp_path / "fetch"
    folder.mkdir()
    checkout = Checkout(folder)
    result = checkout.run(fail_on="dbt_package_archive verify")
    check(result.returncode == 0, f"a failed verification followed by a good fetch must succeed: {result.returncode} {result.stdout + result.stderr}")
    calls = checkout.calls()
    verify = next(index for index, call in enumerate(calls) if "dbt_package_archive verify" in call)
    fetch = next(index for index, call in enumerate(calls) if "dbt_package_archive fetch" in call)
    check(verify < fetch, f"the fetch must follow the failed verification: {calls}")
    check(
        any("pre_commit install" in call for call in calls[fetch:]) and any("install-shim" in call for call in calls[fetch:]),
        "the steps after the fetch must run",
    )

    folder = tmp_path / "both"
    folder.mkdir()
    checkout = Checkout(folder)
    result = checkout.run(fail_on="dbt_package_archive")
    output = result.stdout + result.stderr
    check(result.returncode == 1, f"a failed verification and a failed fetch must exit 1: {result.returncode}")
    check("FAILED at step 10 of 13 (dbt package archive)" in output, f"the failure must name the archive step: {output}")
    calls = checkout.calls()
    check(any("dbt_package_archive fetch" in call for call in calls), f"the fetch must have run: {calls}")
    check(not any("pre_commit" in call or "install-shim" in call for call in calls), f"no later step may run: {calls}")
    check(not any(checkout.temporary.iterdir()), "the temporary folder must be removed")


def test_the_install_script_reached_through_a_symbolic_link_installs_its_own_checkout(tmp_path: Path) -> None:
    checkout = Checkout(tmp_path)
    tools = tmp_path / "tools"
    tools.mkdir()
    (tools / "link").symlink_to(Path("..") / "checkout" / "scripts" / "install.sh")
    environment = {
        "PATH": f"{checkout.bin}{os.pathsep}/usr/bin{os.pathsep}/bin",
        "TMPDIR": str(checkout.temporary),
        "STAND_IN_LOG": str(checkout.log),
        "STAND_IN_PYTHON": str(checkout.python_stand_in),
        "FAIL_ON": "",
        "TERRAFORM_VERSION": "1.16.1",
        "NODE_VERSION": "22.3.0",
        "HAI_DOCKER_APP": str(checkout.docker_app),
    }
    result = run_command("/bin/bash", ["./link"], cwd=tools, env=environment, timeout=60)
    check(result.returncode == 0, f"a linked install must succeed: {result.returncode} {result.stdout + result.stderr}")
    check((checkout.root / ".venv" / "bin" / "python").exists() and (checkout.root / ".env").exists(), "the target checkout must be the one installed")
    check(not (tools / ".venv").exists() and not (tools / ".env").exists() and not (tmp_path / ".venv").exists(), "nothing may be installed beside the link")
