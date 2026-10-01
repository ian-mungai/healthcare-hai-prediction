"""Sanitize the collector AWS environment before using the repository's single launcher."""

import os
from pathlib import Path

from scripts.process import CompletedProcess
from scripts.process import run_command as launch


def run_command(program: str, arguments: list[str], cwd: Path | None = None, timeout: float = 180) -> CompletedProcess[str]:
    """Run a resolved executable with a sanitized AWS environment and captured output."""
    environment = {key: value for key, value in os.environ.items() if not key.startswith("AWS_")}
    environment.update(AWS_IGNORE_CONFIGURED_ENDPOINT_URLS="true", AWS_MAX_ATTEMPTS="3", AWS_RETRY_MODE="standard")
    return launch(program, arguments, cwd=cwd, timeout=timeout, env=environment)
