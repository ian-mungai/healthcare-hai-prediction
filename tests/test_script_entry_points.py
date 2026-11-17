"""The Bash entry points of the README: each runs one existing Python module with the repository's own environment and passes its arguments unchanged.

``scripts/run_checks_e2e.sh``, ``scripts/host.sh`` and ``scripts/open_ui.sh`` are thin: they change to the repository root, refuse a missing ``.venv`` and name
``scripts/install.sh``, and ``exec`` the module with ``"$@"``. They read no argument themselves. The cases run each wrapper from a fake checkout whose
``.venv/bin/python`` records its arguments, so nothing real starts.
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

from scripts.process import CompletedProcess, run_command
from tests.support import check

ROOT = Path(__file__).resolve().parents[1]
WRAPPERS = {
    "run_checks_e2e.sh": "scripts.quality.run_checks_e2e",
    "host.sh": "scripts.infrastructure.host_session",
    "open_ui.sh": "scripts.infrastructure.open_ui",
    "worktree_setup.sh": "scripts.quality.worktree_setup",
}
ARGUMENTS = ["start", "--flag", "a value with spaces", "", "--local-port=8081", "*", "$HOME", "it's"]
PYTHON_STAND_IN = """#!/bin/bash
{ printf '%s\\n' "$PWD"; printf '%s\\n' "$@"; } > "$STAND_IN_LOG"
exit "${STAND_IN_EXIT:-0}"
"""
INTERPRETING = re.compile(r"\b(shift|getopts|case)\b|\$[1-9@*#]|\$\{[1-9]|\$\*")


def fake_checkout(folder: Path, name: str, venv: bool = True) -> Path:
    root = folder / "checkout"
    (root / "scripts").mkdir(parents=True)
    shutil.copy(ROOT / "scripts" / name, root / "scripts" / name)
    if venv:
        (root / ".venv" / "bin").mkdir(parents=True)
        (root / ".venv" / "bin" / "python").write_text(PYTHON_STAND_IN)
        (root / ".venv" / "bin" / "python").chmod(0o755)
    return root


def run_wrapper(name: str, cwd: Path, exit_code: str = "0") -> CompletedProcess[str]:
    """Run the wrapper ``name`` by its path from ``cwd``, a folder inside the fake checkout, as a user might from elsewhere, with the tricky ``ARGUMENTS``.

    The Docker-call guard proves a shell's script and arguments only when they are literal, so each wrapper has its own literal list.
    """
    environment = {"PATH": "/usr/bin:/bin", "STAND_IN_LOG": str(cwd / "calls.log"), "STAND_IN_EXIT": exit_code}
    if name == "host.sh":
        command = ["../scripts/host.sh", "start", "--flag", "a value with spaces", "", "--local-port=8081", "*", "$HOME", "it's"]
    elif name == "open_ui.sh":
        command = ["../scripts/open_ui.sh", "start", "--flag", "a value with spaces", "", "--local-port=8081", "*", "$HOME", "it's"]
    elif name == "worktree_setup.sh":
        command = ["../scripts/worktree_setup.sh", "start", "--flag", "a value with spaces", "", "--local-port=8081", "*", "$HOME", "it's"]
    else:
        command = ["../scripts/run_checks_e2e.sh", "start", "--flag", "a value with spaces", "", "--local-port=8081", "*", "$HOME", "it's"]
    return run_command("/bin/bash", command, cwd=cwd, env=environment, timeout=60)


def test_each_wrapper_calls_an_existing_module_and_is_plain_bash() -> None:
    for name, module in WRAPPERS.items():
        text = (ROOT / "scripts" / name).read_text()
        check(text.startswith("#!/usr/bin/env bash\n"), f"{name}: it must start with the bash shebang")
        check("set -euo pipefail" in text, f"{name}: it must use set -euo pipefail")
        found = re.findall(r"^exec \.venv/bin/python -m ([\w.]+) \"\$@\"$", text, re.MULTILINE)
        check(found == [module], f'{name}: it must exec .venv/bin/python -m {module} "$@" once, found {found}')
        check((ROOT / (module.replace(".", "/") + ".py")).is_file(), f"{name}: the module {module} must exist")
        check("scripts/install.sh" in text, f"{name}: it must name scripts/install.sh when .venv is missing")
        code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#") and "exec .venv/bin/python -m" not in line)
        check(INTERPRETING.search(code) is None, f"{name}: it must not read or change an argument: {INTERPRETING.search(code)}")


def test_arguments_reach_the_module_unchanged_from_the_repository_root(tmp_path: Path) -> None:
    for name, module in WRAPPERS.items():
        folder = tmp_path / name
        folder.mkdir()
        root = fake_checkout(folder, name)
        elsewhere = root / "elsewhere"
        elsewhere.mkdir()
        result = run_wrapper(name, elsewhere)
        check(result.returncode == 0, f"{name}: exit {result.returncode}: {result.stderr}")
        lines = (elsewhere / "calls.log").read_text().split("\n")
        check(lines[0] == str(root.resolve()), f"{name}: it must run from the repository root, ran in {lines[0]}")
        check(lines[1:3] == ["-m", module], f"{name}: it must run the module {module}, ran {lines[1:3]}")
        check(lines[3:-1] == ARGUMENTS, f"{name}: the arguments must pass unchanged:\n{lines[3:-1]}\n{ARGUMENTS}")


def test_the_exit_code_of_the_module_is_the_exit_code_of_the_wrapper(tmp_path: Path) -> None:
    for name in WRAPPERS:
        folder = tmp_path / name
        folder.mkdir()
        root = fake_checkout(folder, name)
        (root / "elsewhere").mkdir()
        result = run_wrapper(name, root / "elsewhere", exit_code="7")
        check(result.returncode == 7, f"{name}: the module exit code 7 must come through, got {result.returncode}")


def test_a_missing_environment_names_the_install_script_and_starts_nothing(tmp_path: Path) -> None:
    for name in WRAPPERS:
        folder = tmp_path / name
        folder.mkdir()
        root = fake_checkout(folder, name, venv=False)
        (root / "elsewhere").mkdir()
        result = run_wrapper(name, root / "elsewhere")
        check(result.returncode == 1, f"{name}: a missing .venv must exit 1: {result.returncode}")
        check("bash scripts/install.sh" in result.stderr, f"{name}: the message must name the install script: {result.stderr}")
        check(not (root / "elsewhere" / "calls.log").exists(), f"{name}: nothing may start without .venv")


def run_ci_trace(extra: dict[str, str]) -> str:
    """The ``bash -x`` trace of a list-only ``scripts/run_ci.sh`` run (it runs no group) under a controlled environment."""
    environment = {"PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin", "HOME": str(ROOT), "CI_LIST_ONLY": "1", "CI_PART": "gates", **extra}
    result = run_command("/bin/bash", ["-x", "scripts/run_ci.sh"], cwd=ROOT, env=environment, timeout=120)
    check(result.returncode == 0, f"a list-only run must succeed: {result.returncode}\n{result.stderr[-2000:]}")
    return result.stdout + result.stderr


def test_run_ci_sets_its_own_plugin_cache_for_a_local_run_and_keeps_a_caller_value() -> None:
    default = f"export TF_PLUGIN_CACHE_DIR={ROOT}/infra/.terraform/providers"
    check(default in run_ci_trace({}), "an unset TF_PLUGIN_CACHE_DIR must default to infra/.terraform/providers in the checkout")
    check("export TF_PLUGIN_CACHE_DIR=" not in run_ci_trace({"TF_PLUGIN_CACHE_DIR": "/caller/cache"}), "a caller's TF_PLUGIN_CACHE_DIR must stay as it is")
    check("export TF_PLUGIN_CACHE_DIR=" not in run_ci_trace({"GITHUB_ACTIONS": "true"}), "hosted CI must keep its own plugin cache choice")
    check(f"python_bin={sys.executable}" in run_ci_trace({"PYTHON_BIN": sys.executable}), "a caller's PYTHON_BIN must stay as it is")
    check(f"python_bin={ROOT}/.venv/bin/python" in run_ci_trace({}), "an unset PYTHON_BIN must default to the checkout's .venv")
