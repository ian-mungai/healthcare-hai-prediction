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
       --image-record-sha256 "$RECORD_SHA"`` and the same with ``--stack jobs`` and ``jobs.tfplan``. Each check reads the saved plan with
       ``terraform show -json`` and refuses it when the record differs from ``RECORD_SHA``, the plan is incomplete or lists no resource, any resource is not
       left alone, the plan belongs to the other stack (its declared variables and every resource it lists must be those of infra/<stack>), or any image
       input of the plan differs from the record, which it names (``image_digests[<name>]`` for services, ``images[<key>]`` and the other image maps for
       jobs). A pass is the evidence that the record and the committed defaults reproduce the live stack as the plans were made. Keep both checks with the
       record digest as the evidence; apply nothing. Run the renderer plan check of each stack too (``--check-plan``), which compares every other input.
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
RESOURCE_BLOCK = re.compile(r'^resource "(?P<type>[a-z0-9_]+)" "(?P<name>[a-z0-9_]+)"', re.MULTILINE)
RESOURCE_ADDRESS = re.compile(r"(?P<base>[a-z0-9_]+\.[a-z0-9_]+)(?P<index>\[.*\])?")
VARIABLE_BLOCK = re.compile(r'^variable "(?P<name>[a-z0-9_]+)" \{\n(?P<body>.*?)^\}\n', re.DOTALL | re.MULTILINE)
SCALAR_DEFAULT = re.compile(r'^\s+default\s*=\s*"(?P<value>[^"\n]*)"\s*$', re.MULTILINE)
MAP_DEFAULT = re.compile(r"^\s+default\s*=\s*\{\n(?P<entries>.*?)^\s*\}\s*$", re.DOTALL | re.MULTILINE)
MAP_ENTRY = re.compile(r'^\s*(?P<key>[A-Za-z0-9_]+)\s*=\s*"(?P<value>[^"\n]*)"\s*$', re.MULTILINE)
LITERAL_DEFAULT = re.compile(r"^\s+default\s*=\s*(?P<literal>true|false|\[[^\]\n]*\])\s*$", re.MULTILINE)


# The committed defaults of these switches and lists, with the overrides on top, are the selection that decides which images and stack outputs are needed.
SELECTION_SETTINGS = (
    "bronze_job_enabled",
    "drift_enabled",
    "host_jobs",
    "host_jobs_enabled",
    "layer_jobs",
    "layer_jobs_enabled",
    "publisher_job_enabled",
    "train_enabled",
)
# Each image input maps its keys to the repository, named <project>_<repository>_<environment>, whose digest the record holds.
JOB_IMAGES = {
    "images": {"analytics": "analytics", "quality": "quality", "spark": "spark"},
    "train_images": {"training": "training", "feature_store": "feature_store"},
    "drift_images": {"training": "training", "feature_store": "feature_store", "drift": "drift"},
    "openmetadata_images": {"ingestion": "openmetadata_ingestion"},
}


def needed_images(selection: dict[str, Any]) -> dict[str, tuple[str, ...]]:
    """The image keys of each image input that the effective selection needs, named as the jobs stack validates them."""
    layers, trains = selection["layer_jobs_enabled"] is True, selection["train_enabled"] is True
    families = set(selection["host_jobs"]) if selection["host_jobs_enabled"] is True else set()
    wanted = {
        "images": {
            *(("analytics",) if selection["publisher_job_enabled"] is True or layers else ()),
            *(("quality",) if layers else ()),
            *(("spark",) if layers or selection["bronze_job_enabled"] is True else ()),
        },
        "train_images": {
            *(("training", "feature_store") if trains else ()),
            *(("feature_store",) if layers and "features" in selection["layer_jobs"] else ()),
        },
        "drift_images": {"training", "feature_store", "drift"} if selection["drift_enabled"] is True else set(),
        "openmetadata_images": {"ingestion"} if "metadata" in families else set(),
    }
    return {group: tuple(key for key in JOB_IMAGES[group] if key in keys) for group, keys in wanted.items() if keys}


def stack_variables(directory: Path) -> dict[str, str]:
    """The body of every variable block declared in the ``.tf`` files directly inside a stack folder."""
    blocks: dict[str, str] = {}
    for source in sorted(directory.glob("*.tf")):
        for found in VARIABLE_BLOCK.finditer(source.read_text(encoding="utf-8")):
            blocks[found["name"]] = found["body"]
    return blocks


def stack_resources(directory: Path) -> set[str]:
    """The ``type.name`` of every resource block declared in the ``.tf`` files directly inside a stack folder."""
    found: set[str] = set()
    for source in sorted(directory.glob("*.tf")):
        found |= {f"{block['type']}.{block['name']}" for block in RESOURCE_BLOCK.finditer(source.read_text(encoding="utf-8"))}
    return found


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
    """Refuse an overrides file that is a symbolic link, and one inside the repository unless Git tracks no part of it and ignores it.

    The path is checked as given, before any normalization: a ``..`` component is refused, because removing it would make the Git checks look at another
    file than the one that opens after a link. A link inside the repository cannot lead the check outside it. A file outside the repository
    cannot enter it.
    """
    refusal = ConfigurationError(
        "A preparation file inside the repository must be untracked and Git-ignored, so it never reaches main. Keep it outside the repository or in an "
        "ignored folder. No Terraform action was run."
    )
    link_refusal = ConfigurationError(
        "A preparation file may not be a symbolic link, because it could point anywhere later. Name the file itself. No Terraform action was run."
    )
    if ".." in path.parts:
        raise ConfigurationError(
            "A preparation file path may not contain a '..' component, because after a link it names another file than the one the Git checks see. "
            "Give the path without it. No Terraform action was run."
        )
    lexical = path.absolute()
    if lexical.is_symlink():
        raise link_refusal
    bases = {root.absolute(), root.resolve()}
    relatives: set[Path] = set()
    for base in bases:
        if lexical.is_relative_to(base):
            relatives.add(lexical.relative_to(base))
            current = base
            for part in lexical.relative_to(base).parts:
                current = current / part
                if current.is_symlink():
                    raise link_refusal
    resolved = path.resolve()
    if resolved.is_relative_to(root.resolve()):
        relatives.add(resolved.relative_to(root.resolve()))
    for relative in sorted(relatives):
        try:
            tracked = run_command("git", ["-C", str(root), "ls-files", "--error-unmatch", "--", str(relative)], timeout=GIT_SECONDS)
            ignored = run_command("git", ["-C", str(root), "check-ignore", "-q", "--", str(relative)], timeout=GIT_SECONDS)
        except (OSError, TimeoutExpired):
            raise refusal from None
        if tracked.returncode == 0 or ignored.returncode != 0:
            raise refusal


def read_record(path: Path) -> tuple[str, dict[str, str]]:
    """The SHA-256 and the digests of the image record, both from one read of its bytes, or a refusal when it cannot be read."""
    refusal = ConfigurationError("The image record could not be read. Record it with push_images record first. No Terraform action was run.")
    try:
        content = path.read_bytes()
        digests = json.loads(content)["image_digests"]
    except (OSError, ValueError, KeyError, TypeError):
        raise refusal from None
    if not isinstance(digests, dict):
        raise refusal
    return hashlib.sha256(content).hexdigest(), {str(name): str(digest) for name, digest in digests.items()}


def plan_variable(plan: dict[str, Any], name: str) -> Any:
    """The value a saved plan was made with for one input: the value it carries, else the default its configuration declares."""
    entry = (plan.get("variables") or {}).get(name)
    if isinstance(entry, dict) and "value" in entry:
        return entry["value"]
    return ((plan.get("configuration") or {}).get("root_module", {}).get("variables", {}).get(name) or {}).get("default")


def verify_plan_stack(plan: dict[str, Any], stack: str, changes: list[Any]) -> None:
    """Refuse a plan that another stack made: its declared variables must be the ones of infra/<stack>, and every resource it lists a resource of that stack."""
    declared = set(stack_variables(INFRA / stack))
    found = set(((plan.get("configuration") or {}).get("root_module") or {}).get("variables") or {})
    set_variables = set(plan.get("variables") or {})
    if found != declared or not set_variables <= declared:
        names = ", ".join(sorted((found ^ declared) | (set_variables - declared))[:5])
        raise ConfigurationError(f"The plan does not belong to the {stack} stack: its variables differ from infra/{stack} ({names}). Nothing was applied.")
    resources = stack_resources(INFRA / stack)
    for change in changes:
        address = str(change.get("address")) if isinstance(change, dict) else str(change)
        parsed = RESOURCE_ADDRESS.fullmatch(address)
        if parsed is None or parsed["base"] not in resources:
            raise ConfigurationError(
                f"The plan lists {address}, which the {stack} stack does not declare, so the plan is of another stack. Nothing was applied."
            )


def image_difference(stack: str, name: str, found: str, recorded: str | None) -> ConfigurationError:
    shown = "absent from the record" if recorded is None else f"{recorded[:19]} in the record"
    return ConfigurationError(
        f"The {stack} plan was made with other images than the record: {name} is {found[:19]} in the plan and {shown}. "
        "Plan again after push_images record. Nothing was applied."
    )


def verify_plan_images(plan: dict[str, Any], stack: str, digests: dict[str, str]) -> int:
    """Compare every image input of the saved plan with the record, image by image, and return how many were compared."""
    compared = 0
    if stack == "services":
        images = plan_variable(plan, "image_digests")
        if not isinstance(images, dict) or not images:
            raise ConfigurationError("The services plan carries no image_digests, so it cannot be bound to the image record. Nothing was applied.")
        for name, digest in sorted(images.items()):
            if digests.get(name) != digest:
                raise image_difference(stack, f"image_digests[{name}]", str(digest), digests.get(name))
            compared += 1
        return compared
    selection = {name: plan_variable(plan, name) for name in SELECTION_SETTINGS}
    project, environment = plan_variable(plan, "project_name"), plan_variable(plan, "environment")
    missing = sorted(name for name, value in {**selection, "project_name": project, "environment": environment}.items() if value is None)
    if missing:
        raise ConfigurationError(f"The jobs plan carries no value for {', '.join(missing)}, so its images cannot be bound to the record. Nothing was applied.")
    for group, keys in needed_images(selection).items():
        present = plan_variable(plan, group)
        for key in keys:
            if not isinstance(present, dict) or key not in present:
                raise ConfigurationError(f"The jobs plan lacks {group}[{key}], which its selection needs. Nothing was applied.")
    for group, members in JOB_IMAGES.items():
        found = plan_variable(plan, group) or {}
        if not isinstance(found, dict):
            raise ConfigurationError(f"The jobs plan input {group} is not a map of images. Nothing was applied.")
        for key, uri in sorted(found.items()):
            if key not in members:
                raise ConfigurationError(f"The jobs plan input {group}[{key}] is not an image of the jobs stack. Nothing was applied.")
            repository = members[key]
            path, _, digest = str(uri).rpartition("@")
            expected = f"{project}_{repository}_{environment}"
            if path.rsplit("/", 1)[-1] != expected:
                raise ConfigurationError(
                    f"The jobs plan input {group}[{key}] names {path.rsplit('/', 1)[-1] or 'no repository'}, not {expected}. Nothing was applied."
                )
            if digests.get(repository) != digest:
                raise image_difference(stack, f"{group}[{key}]", digest, digests.get(repository))
            compared += 1
    return compared


def verify_no_change(plan: Any, record: Path, expected_sha256: str, stack: str) -> tuple[int, int]:
    """Return the resources and the images of a saved plan of ``stack`` that changes nothing and was made with the images of the record.

    It refuses a record other than the given digest, a plan that is incomplete, lists no resource, changes a resource, belongs to another stack or was
    made with an image other than the record's, naming the input that differs.
    """
    if not SHA256_HEX.fullmatch(expected_sha256):
        raise ConfigurationError("Give the image record digest as 64 lowercase hexadecimal characters.")
    actual, digests = read_record(record)
    if actual != expected_sha256:
        raise ConfigurationError("The image record differs from the digest given. Plan again after the record changed. No apply was run.")
    if not isinstance(plan, dict) or plan.get("errored") or not plan.get("complete"):
        raise ConfigurationError("Only a complete, successful saved plan can show no change.")
    changes = plan.get("resource_changes")
    if not isinstance(changes, list) or not changes:
        raise ConfigurationError("The saved plan lists no resource, so it shows nothing. Create a fresh plan of the whole stack.")
    moved = [str(change.get("address")) for change in changes if not isinstance(change, dict) or (change.get("change") or {}).get("actions") != ["no-op"]]
    if moved:
        raise ConfigurationError(
            f"The plan changes {len(moved)} resource(s), the first is {moved[0]}. It does not reproduce the live stack. Nothing was applied."
        )
    verify_plan_stack(plan, stack, changes)
    return len(changes), verify_plan_images(plan, stack, digests)


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
        count, images = verify_no_change(plan, IMAGE_RECORD, arguments.image_record_sha256, arguments.stack)
    except ConfigurationError as error:
        parser.error(str(error))
    sys.stdout.write(
        f"event=no_change_plan stack={arguments.stack} image_record_sha256={arguments.image_record_sha256} resources={count} images={images} changes=0\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
