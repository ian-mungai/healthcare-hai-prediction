"""The stage a check runner passes to ``pre-commit run`` for a hook.

Hooks that left the commit stage live in the manual stage only (.pre-commit-config.yaml), so ``pre-commit run <hook>`` without a stage reports that no such
hook exists. The runners ask this module instead of naming a stage themselves.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from scripts.quality.repo_checks import load_yaml


def hook_stage(config: Path, hook: str) -> str:
    """``pre-commit`` when the hook runs in the commit stage, ``manual`` when only the manual stage holds it; ``pre-commit`` for an unknown hook."""
    document: Any = load_yaml(config.read_text(encoding="utf-8"))
    for repo in document["repos"]:
        for entry in repo["hooks"]:
            if entry["id"] == hook:
                stages = entry.get("stages", document.get("default_stages", ["pre-commit"]))
                return "manual" if "pre-commit" not in stages and "manual" in stages else "pre-commit"
    return "pre-commit"
