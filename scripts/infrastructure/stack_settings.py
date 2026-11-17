"""Shared inputs of the services and jobs renderers, and the no-change plan check bound to the image record.

Owner steps that refresh the image record and prove the committed defaults still reproduce the live stacks. Run them from the repository root in one
shell, under the project profile, with ``.env`` set:

    1. Refresh the record of pushed image digests (infra/services/images.auto.tfvars.json, Git-ignored). It must name every image of the default selection:
       analytics, quality, spark, training, feature_store, drift and openmetadata_ingestion for the jobs stack, and the service images for the services
       stack, each at the digest the live task definitions run. ``.venv/bin/python -m scripts.infrastructure.push_images plan`` prints the commands that
       read each digest back from ECR, and ``push_images record NAME=sha256:<DIGEST> ...`` merges them into the record; a render that finds an image
       missing or malformed names it and writes nothing.
    2. Take the record's digest: ``RECORD_SHA=$(shasum -a 256 infra/services/images.auto.tfvars.json | cut -d' ' -f1)``.
    3. Render and plan each stack, saving the plan: ``bash scripts/infrastructure/terraform.sh --services plan -out services.tfplan`` and
       ``bash scripts/infrastructure/terraform.sh --jobs plan -out jobs.tfplan``.
    4. Check each plan: ``.venv/bin/python -m scripts.infrastructure.stack_settings --stack services --plan services.tfplan
       --image-record-sha256 "$RECORD_SHA"`` and the same with ``--stack jobs`` and ``jobs.tfplan``. Each check refuses a record whose digest differs from
       ``RECORD_SHA`` and any plan that changes a resource, so a pass is the evidence that the record and the committed defaults reproduce the live
       stack. Keep both plan checks with the record digest as the evidence; apply nothing.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError
from scripts.process import TimeoutExpired, run_command

INFRA = REPO_ROOT / "infra"
IMAGE_RECORD = INFRA / "services" / "images.auto.tfvars.json"
DEFAULT_PROJECT = "healthcare_hai_prediction"
NAMESPACE = f"/{DEFAULT_PROJECT}/"
GIT_SECONDS = 30
TERRAFORM_SECONDS = 120
SHA256_HEX = re.compile(r"[0-9a-f]{64}")
VARIABLE_BLOCK = re.compile(r'^variable "(?P<name>[a-z0-9_]+)" \{\n(?P<body>.*?)^\}\n', re.DOTALL | re.MULTILINE)
SCALAR_DEFAULT = re.compile(r'^\s+default\s*=\s*"(?P<value>[^"\n]*)"\s*$', re.MULTILINE)
MAP_DEFAULT = re.compile(r"^\s+default\s*=\s*\{\n(?P<entries>.*?)^\s*\}\s*$", re.DOTALL | re.MULTILINE)
MAP_ENTRY = re.compile(r'^\s*(?P<key>[A-Za-z0-9_]+)\s*=\s*"(?P<value>[^"\n]*)"\s*$', re.MULTILINE)
LITERAL_DEFAULT = re.compile(r"^\s+default\s*=\s*(?P<literal>true|false|\[[^\]\n]*\])\s*$", re.MULTILINE)


def stack_variables(directory: Path) -> dict[str, str]:
    """The body of every variable block declared in the ``.tf`` files directly inside a stack folder."""
    blocks: dict[str, str] = {}
    for source in sorted(directory.glob("*.tf")):
        for found in VARIABLE_BLOCK.finditer(source.read_text(encoding="utf-8")):
            blocks[found["name"]] = found["body"]
    return blocks


def parameter_defaults(directory: Path) -> dict[str, str | dict[str, str]]:
    """The defaults of a stack's variables that name a Parameter Store path of the default project: a string, or the strings of a map."""
    defaults: dict[str, str | dict[str, str]] = {}
    for name, body in stack_variables(directory).items():
        scalar = SCALAR_DEFAULT.search(body)
        mapping = MAP_DEFAULT.search(body)
        if scalar is not None and scalar["value"].startswith(NAMESPACE):
            defaults[name] = scalar["value"]
        elif mapping is not None:
            entries = {entry["key"]: entry["value"] for entry in MAP_ENTRY.finditer(mapping["entries"])}
            if any(value.startswith(NAMESPACE) for value in entries.values()):
                defaults[name] = entries
    return defaults


def for_project(value: str, project: str) -> str:
    """The Parameter Store path of the default project, moved to the namespace of ``project``."""
    return f"/{project}/{value.removeprefix(NAMESPACE)}" if value.startswith(NAMESPACE) else value


def project_parameter_values(directory: Path, project: str) -> dict[str, Any]:
    """Every project-bound parameter name of a stack for ``project``: nothing for the default project, whose committed defaults already hold them."""
    if project == DEFAULT_PROJECT:
        return {}
    defaults = parameter_defaults(directory)
    if not defaults:
        raise ConfigurationError(
            f"The {directory.name} stack variables could not be read, so its parameter names cannot follow the project. No Terraform action ran."
        )
    return {
        name: {key: for_project(item, project) for key, item in value.items()} if isinstance(value, dict) else for_project(value, project)
        for name, value in defaults.items()
    }


def parameter_name(source: Path, variable: str, project: str) -> str:
    """The Parameter Store path a variable defaults to in one Terraform file, moved to the namespace of ``project``."""
    body = stack_variables(source.parent).get(variable) if source.parent.is_dir() else None
    scalar = SCALAR_DEFAULT.search(body) if body is not None else None
    if scalar is None:
        raise ConfigurationError(f"{source.name} has no default for {variable}")
    return for_project(scalar["value"], project)


def terraform_default(directory: Path, name: str) -> Any:
    """The committed one-line true, false or list default of a variable, which applies while no input names the variable."""
    found = LITERAL_DEFAULT.search(stack_variables(directory).get(name, ""))
    if found is None:
        raise ConfigurationError(f"{directory.name} has no true, false or list default for {name}")
    return json.loads(found["literal"])


def require_local_override(path: Path, root: Path = REPO_ROOT) -> None:
    """Refuse an overrides file inside the repository unless Git tracks no part of it and ignores it; a file outside the repository cannot enter it."""
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(root.resolve())
    except ValueError:
        return
    refusal = ConfigurationError(
        "A preparation file inside the repository must be untracked and Git-ignored, so it never reaches main. Keep it outside the repository or in an "
        "ignored folder. No Terraform action was run."
    )
    try:
        tracked = run_command("git", ["-C", str(root), "ls-files", "--error-unmatch", "--", str(relative)], timeout=GIT_SECONDS)
        ignored = run_command("git", ["-C", str(root), "check-ignore", "-q", "--", str(relative)], timeout=GIT_SECONDS)
    except (OSError, TimeoutExpired):
        raise refusal from None
    if tracked.returncode == 0 or ignored.returncode != 0:
        raise refusal


def record_sha256(path: Path = IMAGE_RECORD) -> str:
    """The SHA-256 of the image record's exact bytes, or a refusal when it cannot be read."""
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        raise ConfigurationError("The image record could not be read. Record it with push_images record first. No Terraform action was run.") from None


def verify_no_change(plan: Any, record: Path, expected_sha256: str) -> int:
    """Return the number of resources of a complete saved plan, refusing a record other than the given digest and any plan that changes a resource."""
    if not SHA256_HEX.fullmatch(expected_sha256):
        raise ConfigurationError("Give the image record digest as 64 lowercase hexadecimal characters.")
    if record_sha256(record) != expected_sha256:
        raise ConfigurationError("The image record differs from the digest given. Plan again after the record changed. No apply was run.")
    if not isinstance(plan, dict) or plan.get("errored") or not plan.get("complete"):
        raise ConfigurationError("Only a complete, successful saved plan can show no change.")
    changes = plan.get("resource_changes")
    if not isinstance(changes, list):
        raise ConfigurationError("The saved plan lists no resource changes. Create a fresh plan.")
    moved = [str(change.get("address")) for change in changes if not isinstance(change, dict) or (change.get("change") or {}).get("actions") != ["no-op"]]
    if moved:
        raise ConfigurationError(
            f"The plan changes {len(moved)} resource(s), the first is {moved[0]}. It does not reproduce the live stack. Nothing was applied."
        )
    return len(changes)


def main() -> int:
    """Check that a saved plan of the services or jobs stack changes nothing and was made with the image record of the given digest."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("--stack", choices=("services", "jobs"), required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--image-record-sha256", required=True, help="the SHA-256 of infra/services/images.auto.tfvars.json the plan was made with")
    arguments = parser.parse_args()
    try:
        try:
            result = run_command("terraform", [f"-chdir={INFRA / arguments.stack}", "show", "-json", str(arguments.plan.resolve())], timeout=TERRAFORM_SECONDS)
        except (OSError, TimeoutExpired):
            raise ConfigurationError("The saved plan could not be inspected within 120 seconds. Nothing was applied.") from None
        if result.returncode:
            raise ConfigurationError("The saved plan could not be inspected. Nothing was applied.")
        try:
            plan = json.loads(result.stdout)
        except ValueError:
            raise ConfigurationError("The saved plan is not valid JSON. Nothing was applied.") from None
        count = verify_no_change(plan, IMAGE_RECORD, arguments.image_record_sha256)
    except ConfigurationError as error:
        parser.error(str(error))
    sys.stdout.write(f"event=no_change_plan stack={arguments.stack} image_record_sha256={arguments.image_record_sha256} resources={count} changes=0\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
