from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

from tests.support import check

ROOT = Path(__file__).resolve().parents[3]
ENTRY = ("scripts.acquisition.redownload", "scripts.acquisition.stored_mirror")
DYNAMIC = [
    "collect_bls_api",
    "collect_census_acs_api",
    "collect_census_acs_detailed",
    "collect_hud_api",
    "collect_mmd_api",
    "store_hud_xlsx",
    "store_wonder_export",
    "store_cms_owners",
    "store_hcai_util",
    "store_onc_mu",
    "store_reference_download",
    "compare_mmd_api",
    "history_redirects",
    "hud_xlsx_contract",
    "wonder_export_contract",
    "onc_mu_contract",
    "hcai_util_contract",
    "cms_owners_contract",
    "census_acs_detailed_contract",
    "census_acs_api_contract",
    "hud_api_contract",
    "mmd_api_contract",
    "bls_api_contract",
    "reference_download_contract",
    "single_copy",
    "legacy_versions",
    "code_versions",
    "capture",
    "source_metadata",
]


# Import names that differ from their package names.
PACKAGE_OF = {"dotenv": "python-dotenv"}


def module_file(name: str) -> Path | None:
    base = ROOT.joinpath(*name.split("."))
    return next((candidate for candidate in (base.with_suffix(".py"), base / "__init__.py") if candidate.is_file()), None)


def third_party_imports() -> set[str]:
    """Top-level packages the task's redownload code imports, found by reading the source the way the worker snapshot does."""
    seen: set[str] = set()
    pending = [*ENTRY, *[f"scripts.acquisition.{name}" for name in DYNAMIC]]
    found: set[str] = set()
    while pending:
        name = pending.pop()
        source = module_file(name)
        if name in seen or source is None:
            continue
        seen.add(name)
        for node in ast.walk(ast.parse(source.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                names = [node.module, *[f"{node.module}.{alias.name}" for alias in node.names]]
            else:
                continue
            for imported in names:
                top = imported.split(".")[0]
                if top == "scripts":
                    pending.append(imported)
                elif top not in sys.stdlib_module_names:
                    found.add(top)
    return found


def locked_packages(path: Path) -> set[str]:
    return {name.lower().replace("_", "-") for name in re.findall(r"^([A-Za-z0-9_.-]+)==", path.read_text(encoding="utf-8"), re.M)}


def declared(path: Path) -> set[str]:
    lines = [line.split("#")[0].strip() for line in path.read_text(encoding="utf-8").splitlines()]
    return {re.split(r"[<>=!~\[ ]", line)[0].lower().replace("_", "-") for line in lines if line}


def test_the_task_code_imports_only_packages_the_image_will_hold() -> None:
    analytics = ROOT / "services/analytics"
    allowed = locked_packages(analytics / "requirements.txt") | declared(analytics / "requirements.publisher.in")
    missing = sorted(name for name in third_party_imports() if PACKAGE_OF.get(name, name).lower().replace("_", "-") not in allowed)
    check(missing == [], f"the image lacks {missing}")


def test_the_publisher_dependencies_are_declared_for_a_hash_locked_install() -> None:
    declared_names = declared(ROOT / "services/analytics/requirements.publisher.in")
    check({"boto3", "defusedxml"} <= declared_names, "boto3 and defusedxml are declared")
    dockerfile = (ROOT / "services/analytics/Dockerfile").read_text(encoding="utf-8")
    check(
        "--require-hashes" in dockerfile and "requirements.publisher.txt" in dockerfile and "PUBLISHER_DEPENDENCIES" in dockerfile,
        "the install is hash locked and optional",
    )


def environment_names_the_task_definition_sets() -> set[str]:
    text = (ROOT / "infra/jobs/main.tf").read_text(encoding="utf-8")
    return set(re.findall(r'name\s*=\s*"([A-Z][A-Z0-9_]+)"', text))


def test_every_variable_the_task_code_reads_is_set_by_the_task_definition() -> None:
    from scripts.acquisition import collection_layout, raw_root, stored_mirror, transport

    needed = {
        transport.APPROVED_HOSTS_VARIABLE,
        transport.APPROVED_HOSTS_APPROVAL_VARIABLE,
        collection_layout.CAPTURE_ROOT_VARIABLE,
        *stored_mirror.REQUIRED_ENVIRONMENT,
    }
    missing = sorted(needed - environment_names_the_task_definition_sets())
    check(missing == [], f"the task definition does not set {missing}")
    check(raw_root.READ_RAW_VARIABLE not in environment_names_the_task_definition_sets(), "the publisher task reads no bronze root variable")


def dockerfile_lines() -> list[str]:
    text = (ROOT / "services/analytics/Dockerfile").read_text(encoding="utf-8")
    return [line.strip() for line in text.replace("\\\n", " ").splitlines()]


def test_every_file_the_default_build_copies_exists() -> None:
    folder = ROOT / "services/analytics"
    sources = []
    for line in dockerfile_lines():
        parts = line.split()
        if parts and parts[0] == "COPY" and not any(part.startswith("--from=") for part in parts):
            sources += [part for part in parts[1:-1] if not part.startswith("--")]
    missing = sorted(name for name in sources if not list(folder.glob(name)))
    check(missing == [], f"the default build copies files that do not exist: {missing}")


def test_the_publisher_lock_is_read_only_inside_the_conditional_install() -> None:
    lines = dockerfile_lines()
    mentions = [line for line in lines if "requirements.publisher.txt" in line and not line.startswith("#")]
    check(len(mentions) == 1, "the lock is named in exactly one instruction")
    check(
        mentions[0].startswith("RUN --mount=type=bind") and 'if [ "$PUBLISHER_DEPENDENCIES" = "1" ]' in mentions[0] and "--require-hashes" in mentions[0],
        "it is read from the build context only when asked for, with hashes",
    )
    check(not any(line.startswith("COPY") and "publisher" in line for line in lines), "no COPY names a publisher file")
