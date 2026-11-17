"""Write the reviewed services preparation inputs file for the code one-shot from the complete existing services settings.

The services renderer treats a preparation file as the whole set of nondefault services inputs, so a file that holds only the code one-shot role and bucket
silently resets every other setting (the MLflow switch, for example) to its default. This tool starts from the existing rendered inputs of the services
stack (``infra/services/deployment.auto.tfvars.json``), keeps every setting that the preparation schema supports, sets the code one-shot role and bucket and
validates the result against that schema. It prints setting names only, never values, because the values hold role ARNs.

Run from the repository root: ``.venv/bin/python -m scripts.infrastructure.services_code_inputs --existing infra/services/deployment.auto.tfvars.json
--role-file <host_code_fetch_role output> --bucket-file <data bucket name file> --output <services_code.json>``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from scripts.infrastructure.render_platform_config import PLATFORM_KEYS, PREPARATION_SCHEMAS, SERVICES_PLATFORM_KEYS

REPO_ROOT = Path(__file__).resolve().parents[2]
SERVICES_DIRECTORY = REPO_ROOT / "infra" / "services"
# The two settings this tool sets, and the image record that the renderer reads from its own file.
INSTALLER_SETTINGS = ("code_fetch_task_role_arn", "code_bucket_name")
# The setting the same tool sets for the catalog setup one-shot when it is given that role; without the role an existing value is kept as it is.
CATALOG_SETUP_SETTING = "catalog_setup_task_role_arn"
RENDERED_SEPARATELY = ("image_digests",)


class InputsRefused(Exception):
    """The inputs file cannot be written safely."""


def supported_variables() -> set[str]:
    """The settings that a services preparation file can carry."""
    schema = json.loads(PREPARATION_SCHEMAS["services"].read_text(encoding="utf-8"))
    return set(schema["properties"]["variables"]["properties"])


def declared_variables() -> set[str]:
    """The input variables that the services stack declares."""
    pattern = re.compile(r'^variable\s+"([a-z0-9_]+)"', re.MULTILINE)
    return {name for path in sorted(SERVICES_DIRECTORY.glob("*.tf")) for name in pattern.findall(path.read_text(encoding="utf-8"))}


def rendered_variables() -> set[str]:
    """The settings that the renderer fills from .env, the platform outputs and the image record, so no preparation file carries them."""
    return {*PLATFORM_KEYS, *SERVICES_PLATFORM_KEYS, *RENDERED_SEPARATELY}


def read_object(path: Path, what: str) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise InputsRefused(f"the {what} file cannot be read as JSON: {path}") from None
    if not isinstance(document, dict):
        raise InputsRefused(f"the {what} file is not a JSON object: {path}")
    return document


def build(existing: dict[str, Any], role_arn: str, bucket: str, catalog_setup_role_arn: str | None = None) -> tuple[dict[str, Any], dict[str, list[str]]]:
    """The preparation document and a report of setting names: kept, set, left to the renderer and ignored."""
    supported, declared, rendered = supported_variables(), declared_variables(), rendered_variables()
    blocked = sorted(name for name in existing if name in declared and name not in supported and name not in rendered)
    if blocked:
        raise InputsRefused(f"the services stack declares these existing settings, but no preparation file can carry them: {', '.join(blocked)}. Stop and ask")
    new = {"code_fetch_task_role_arn": role_arn, "code_bucket_name": bucket}
    if catalog_setup_role_arn is not None:
        new[CATALOG_SETUP_SETTING] = catalog_setup_role_arn
    variables = {name: value for name, value in existing.items() if name in supported and name not in new}
    report = {
        "kept": sorted(variables),
        "set": sorted(name for name, value in new.items() if existing.get(name) != value),
        "unchanged installer settings": sorted(name for name, value in new.items() if existing.get(name) == value),
        "left to the renderer": sorted(name for name in existing if name in rendered),
        "ignored, not declared by the services stack": sorted(name for name in existing if name not in supported | declared | rendered),
    }
    variables.update(new)
    document = {"schema_version": 1, "root": "services", "variables": dict(sorted(variables.items()))}
    schema = json.loads(PREPARATION_SCHEMAS["services"].read_text(encoding="utf-8"))
    invalid = next(Draft202012Validator(schema).iter_errors(document), None)
    if invalid is not None:
        raise InputsRefused(
            f"the inputs do not satisfy the services preparation schema at {'/'.join(str(part) for part in invalid.absolute_path) or 'the document'}"
        )
    return document, report


def main(argv: list[str] | None = None) -> int:
    """Write the file and print the setting names; return 0 on success and 1 when the inputs cannot be written safely."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--existing", type=Path, required=True, help="the complete existing rendered services inputs")
    parser.add_argument("--role-file", type=Path, required=True, help="the host_code_fetch_role output of the IAM stack (JSON with an arn)")
    parser.add_argument("--bucket-file", type=Path, required=True, help="a file holding the data bucket name")
    parser.add_argument(
        "--catalog-setup-role-file", type=Path, help="the host_catalog_setup_role output of the IAM stack (JSON with an arn); sets the catalog setup one-shot"
    )
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args(argv)
    try:
        existing = read_object(arguments.existing, "existing inputs")
        role = read_object(arguments.role_file, "role").get("arn")
        try:
            bucket = arguments.bucket_file.read_text(encoding="utf-8").strip()
        except OSError:
            raise InputsRefused(f"the bucket file cannot be read: {arguments.bucket_file}") from None
        if not isinstance(role, str) or not bucket:
            raise InputsRefused("the role file needs an arn string and the bucket file a bucket name")
        catalog_setup = None
        if arguments.catalog_setup_role_file is not None:
            catalog_setup = read_object(arguments.catalog_setup_role_file, "catalog setup role").get("arn")
            if not isinstance(catalog_setup, str):
                raise InputsRefused("the catalog setup role file needs an arn string")
        document, report = build(existing, role, bucket, catalog_setup)
        staged = arguments.output.with_name(arguments.output.name + ".next")
        staged.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
        staged.chmod(0o600)
        os.replace(staged, arguments.output)
    except InputsRefused as error:
        sys.stderr.write(f"services_code_inputs refused: {error}\n")
        return 1
    except OSError as error:
        sys.stderr.write(f"services_code_inputs refused: the output cannot be written: {type(error).__name__}\n")
        return 1
    for name, names in report.items():
        sys.stdout.write(f"{name}: {', '.join(names) if names else 'none'}\n")
    sys.stdout.write(f"wrote {arguments.output}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
