"""Give a synthetic renderer workspace what the renderers read from the repository: the stack variable files and a Git repository that ignores all files."""

from __future__ import annotations

import shutil
from pathlib import Path

from scripts.process import run_command

ROOT = Path(__file__).resolve().parents[2]
GIT_SECONDS = 30


def add_repository_files(workspace: Path) -> None:
    """Copy the services and jobs variable files into ``workspace`` and make it a Git repository that ignores everything in it, as an overrides folder is."""
    for stack in ("services", "jobs"):
        target = workspace / "infra" / stack
        target.mkdir(parents=True, exist_ok=True)
        for source in sorted((ROOT / "infra" / stack).glob("*.tf")):
            shutil.copy2(source, target / source.name)
    if run_command("git", ["-C", str(workspace), "init", "-q"], timeout=GIT_SECONDS).returncode:
        raise RuntimeError("the synthetic workspace could not become a Git repository")
    (workspace / ".gitignore").write_text("*\n", encoding="utf-8")
