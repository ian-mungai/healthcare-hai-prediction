"""Render the per-service IAM policy templates in ``infra/iam/policies/`` with the values of ``.env``.

A template holds ``${AWS_REGION}``, ``${AWS_ACCOUNT_ID}``, ``${PROJECT_NAME}``, ``${ENVIRONMENT}`` and ``${DATA_BUCKET_NAME}``; IAM policy variables such as
``${aws:username}`` stay. An unresolved or unknown placeholder, a document that is not a policy and a policy over the managed policy size limit stop the render.

The secret, parameter and lakehouse role names are not in any template. A list element that holds ``${SHARED_SECRET_NAME}``, ``${READ_PARAMETER_NAME}``
or ``${MANAGED_PARAMETER_NAME}`` is repeated once per name. A single name is a placeholder that is its variable's name in upper case, for example
``${LAKEHOUSE_READER_ROLE_NAME}`` or ``${POSTGRES_PASSWORD_NAME}``. All of them are the defaults of the root variables in ``infra/iam/variables.tf``, the same
variables the Terraform stack and ``scripts/infrastructure/secret_store.py`` use. Add a secret there and its policy follows.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from scripts.infrastructure.render_project_config import REPO_ROOT, ConfigurationError, load_configuration
from scripts.infrastructure.secret_store import SecretStoreError, policy_names, variable_names

POLICIES_DIRECTORY = Path(__file__).resolve().parents[1] / "policies"
VARIABLES_FILE = Path(__file__).resolve().parents[1] / "variables.tf"
PLACEHOLDER = re.compile(r"\$\{([A-Z][A-Z0-9_]*)\}")
TEMPLATE_PREFIX = "healthcare_hai_prediction_"
PLACEHOLDER_NAMES = ("AWS_REGION", "AWS_ACCOUNT_ID", "PROJECT_NAME", "ENVIRONMENT", "DATA_BUCKET_NAME")
NAME_PLACEHOLDERS = ("SHARED_SECRET_NAME", "READ_PARAMETER_NAME", "MANAGED_PARAMETER_NAME")
MAX_POLICY_CHARACTERS = 6144


class PolicyRenderError(ValueError):
    """A policy template holds an unresolved or unknown placeholder, is not a policy document or is over the size limit."""


def placeholder_values(storage: Mapping[str, str]) -> dict[str, str]:
    """The placeholder values of a validated ``.env`` configuration (``render_project_config.load_configuration``)."""
    return {
        "AWS_REGION": storage["aws_region"],
        "AWS_ACCOUNT_ID": storage["expected_account_id"],
        "PROJECT_NAME": storage["project_name"],
        "ENVIRONMENT": storage["environment"],
        "DATA_BUCKET_NAME": storage["data_bucket_name"],
    }


def non_whitespace_length(document: Mapping[str, Any]) -> int:
    """The size IAM counts against the 6,144 character limit of a managed policy: the compact JSON without white space."""
    return len(re.sub(r"\s", "", json.dumps(document, separators=(",", ":"))))


def terraform_names(variables: Path = VARIABLES_FILE) -> dict[str, Sequence[str]]:
    """The names of the Terraform variable defaults, by placeholder: the three name lists, and each single name as a string."""
    try:
        return {**policy_names(variables), **variable_names(variables)}
    except (OSError, SecretStoreError) as error:
        raise PolicyRenderError(f"the names cannot be read from {variables.name}: {error}") from None


def expand_names(node: Any, names: Mapping[str, Sequence[str]]) -> Any:
    """Repeat every list element that holds a name placeholder once per name; a name placeholder anywhere else is refused."""
    if isinstance(node, dict):
        return {key: expand_names(child, names) for key, child in node.items()}
    if not isinstance(node, list):
        if isinstance(node, str) and set(PLACEHOLDER.findall(node)) & set(NAME_PLACEHOLDERS):
            raise PolicyRenderError("a name placeholder must be an element of a list")
        return node
    expanded: list[Any] = []
    for item in node:
        used = sorted(set(PLACEHOLDER.findall(item)) & set(NAME_PLACEHOLDERS)) if isinstance(item, str) else []
        if len(used) > 1:
            raise PolicyRenderError(f"one list element holds more than one name placeholder: {', '.join(used)}")
        if not used:
            expanded.append(expand_names(item, names))
            continue
        if not names.get(used[0]):
            raise PolicyRenderError(f"no names for: {used[0]}")
        expanded += [item.replace("${" + used[0] + "}", name) for name in names[used[0]]]
    return expanded


def render_template(text: str, values: Mapping[str, str], names: Mapping[str, Sequence[str]] | None = None) -> dict[str, Any]:
    """Replace every upper-case ``${NAME}`` placeholder, repeat the list elements of the name placeholders and parse the policy.

    ``names`` replaces the Terraform variable defaults as the source of the names; only a test passes it.
    """
    found = set(PLACEHOLDER.findall(text))
    scalars = found - set(PLACEHOLDER_NAMES) - set(NAME_PLACEHOLDERS)
    if scalars or found & set(NAME_PLACEHOLDERS):
        names = terraform_names() if names is None else names
    unknown = sorted(scalars - {key for key in (names or {}) if key not in NAME_PLACEHOLDERS})
    if unknown:
        raise PolicyRenderError(f"unknown placeholders: {', '.join(unknown)}")
    missing = sorted(name for name in found & set(PLACEHOLDER_NAMES) if not values.get(name))
    if missing:
        raise PolicyRenderError(f"missing values for: {', '.join(missing)}")
    single: dict[str, str] = {}
    for name in scalars:
        value = names[name] if names is not None else ""
        if not isinstance(value, str):
            raise PolicyRenderError("a single name placeholder needs one name, not a list")
        single[name] = value
    document = json.loads(PLACEHOLDER.sub(lambda match: values.get(match.group(1)) or single.get(match.group(1)) or match.group(0), text))
    if found & set(NAME_PLACEHOLDERS) and names is not None:
        document = expand_names(document, names)
    if not isinstance(document, dict) or not isinstance(document.get("Statement"), list):
        raise PolicyRenderError("a policy document must be a JSON object with a Statement list")
    if non_whitespace_length(document) > MAX_POLICY_CHARACTERS:
        raise PolicyRenderError(f"the rendered policy has {non_whitespace_length(document)} characters; IAM allows {MAX_POLICY_CHARACTERS}")
    return document


def render_policy_document(source: Path, values: Mapping[str, str], names: Mapping[str, Sequence[str]] | None = None) -> dict[str, Any]:
    """Render one template file."""
    try:
        return render_template(source.read_text(encoding="utf-8"), values, names)
    except PolicyRenderError as error:
        raise PolicyRenderError(f"{source.name}: {error}") from None


def template_service(stem: str) -> str:
    """The service of a template file stem, ``healthcare_hai_prediction_<service>_policy``."""
    return stem.removeprefix(TEMPLATE_PREFIX).removesuffix("_policy")


def load_documents(
    values: Mapping[str, str],
    services: set[str] | None = None,
    directory: Path = POLICIES_DIRECTORY,
    names: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Render every ``*_policy.json`` (or the selected services) in the directory, keyed by service."""
    paths = {template_service(path.stem): path for path in sorted(directory.glob(f"{TEMPLATE_PREFIX}*_policy.json"))}
    if not paths:
        raise PolicyRenderError(f"no policy templates in {directory.name}")
    if services:
        unknown = services - paths.keys()
        if unknown:
            raise PolicyRenderError(f"unknown policy templates: {', '.join(sorted(unknown))}")
    return {service: render_policy_document(path, values, names) for service, path in paths.items() if not services or service in services}


def main() -> None:
    """Write the rendered policies to an owner-only directory outside Git, for review."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=REPO_ROOT / ".env")
    parser.add_argument(
        "--output-directory", type=Path, required=True, help="Directory for the rendered policies; it holds account values, so keep it out of Git."
    )
    parser.add_argument("--policy", action="append", dest="policies", help="Limit the render to one service, for example sns. Repeat as needed.")
    arguments = parser.parse_args()
    try:
        storage, _ = load_configuration(arguments.env_file)
        documents = load_documents(placeholder_values(storage), set(arguments.policies) if arguments.policies else None)
    except (ConfigurationError, PolicyRenderError, OSError) as error:
        parser.error(str(error))
    output = arguments.output_directory.expanduser().absolute()
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    for service, document in documents.items():
        target = output / f"{TEMPLATE_PREFIX}{service}_policy.json"
        target.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
        target.chmod(0o600)
        sys.stdout.write(f"Rendered {target.stem}: {non_whitespace_length(document)} of {MAX_POLICY_CHARACTERS} characters\n")


if __name__ == "__main__":
    main()
