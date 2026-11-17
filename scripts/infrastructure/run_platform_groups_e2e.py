"""Prove that the platform stack refuses a repository group it does not know, and that no input of the real stack can name another image list.

The platform stack makes one ECR repository for each image of ``infra/services/images.json`` and leaves out the monitoring and serving images while their
switches are false. Every image names its ``repository_group`` there, and a lifecycle precondition on the repository resource stops every plan on a group that
is none of ``always``, ``monitoring`` or ``serving``. The stack reads that one file from a fixed path, so no variable, test or override of the real stack can
swap the list.

The run makes disposable copies of the stack (its folder, the image list and the policy file its tests read) in a scratch folder, replaces the image list in a
copy and runs ``terraform test`` there with a mocked provider and no AWS call. The first run of the test file plans the whole stack, so a refused plan fails it
and skips the rest. Each case runs twice: with the committed defaults and with ``monitoring_enabled`` false (the switch that leaves a monitoring image out).

1. Control: the unchanged copy passes every test of the file.
2. A misspelled, capitalized, padded, empty, missing, null, number, boolean, list or object group of a monitoring, a serving or an always image fails both runs
   with the precondition message that names the image.
3. The real stack declares no variable that names another image list, reads no file but the image list, and makes repositories only from its keys.
4. Every Terraform call uses a plugin cache inside the run's scratch folder, so the caller's ``TF_PLUGIN_CACHE_DIR`` is left unchanged.

Run: ``.venv/bin/python -m scripts.infrastructure.run_platform_groups_e2e --output-directory data/e2e/<new folder>``
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

from scripts.infrastructure import run_serving_release_real_e2e as stack

REPO_ROOT = stack.REPO_ROOT
PLATFORM = REPO_ROOT / "infra" / "platform"
MANIFEST = REPO_ROOT / "infra" / "services" / "images.json"
POLICY = REPO_ROOT / "infra" / "iam" / "policies" / "healthcare_hai_prediction_sns_policy.json"
TEST_FILE = "tests/platform.tftest.hcl"
MESSAGE = "Every image of infra/services/images.json needs a repository_group of always, monitoring or serving."
MISSING = object()
# The image, the value of its group and the words that describe the case. Each value is none of the three groups, whatever the image.
BAD_GROUPS: list[tuple[str, str, Any]] = [
    ("misspelled monitoring group", "prometheus", "monitorng"),
    ("capitalized monitoring group", "grafana", "Monitoring"),
    ("monitoring group with a trailing space", "statsd_exporter", "monitoring "),
    ("capitalized serving group", "model_api", "Serving"),
    ("serving group of another name", "streamlit", "serve"),
    ("unknown group of an always image", "postgres", "base"),
    ("empty group", "redis", ""),
    ("missing group", "prometheus", MISSING),
    ("null group", "prometheus", None),
    ("number group", "prometheus", 5),
    ("boolean group", "polaris", True),
    ("list group", "prometheus", ["monitoring"]),
    ("object group", "prometheus", {"group": "monitoring"}),
]


def mutated(image: str, value: Any) -> str:
    """The image list with the group of one image replaced, or removed for the missing value."""
    document = json.loads(MANIFEST.read_text(encoding="utf-8"))
    if value is MISSING:
        del document["images"][image]["repository_group"]
    else:
        document["images"][image]["repository_group"] = value
    return json.dumps(document, indent=2) + "\n"


def copy_stack(destination: Path, manifest: str | None, plugin_directory: Path | None, cache: Path) -> Path:
    """Copy the platform stack beside the image list and the policy file its tests read, with the list replaced when given, and initialize the copy offline."""
    root = destination / "infra"
    shutil.copytree(
        PLATFORM, root / "platform", ignore=shutil.ignore_patterns(".terraform", "*.tfstate*", "*.tfplan", "ci.tfvars.json", "deployment.auto.tfvars.json")
    )
    (root / "services").mkdir()
    (root / "services" / "images.json").write_text(manifest if manifest is not None else MANIFEST.read_text(encoding="utf-8"), encoding="utf-8")
    (root / "iam" / "policies").mkdir(parents=True)
    shutil.copy2(POLICY, root / "iam" / "policies" / POLICY.name)
    arguments = ["init", "-backend=false", "-input=false", "-no-color"]
    if plugin_directory is not None:
        arguments.append(f"-plugin-dir={plugin_directory}")
    elif (root / "platform" / ".terraform.lock.hcl").is_file():
        arguments.append("-lockfile=readonly")
    code, text = stack.terraform(arguments, root / "platform", {"TF_PLUGIN_CACHE_DIR": str(cache)})
    if code != 0:
        raise RuntimeError(f"terraform init failed:\n{text[-1500:]}")
    return root / "platform"


def run_tests(directory: Path, monitoring: bool, cache: Path) -> tuple[int, str]:
    """Run the test file of the copy with the committed defaults, or with the monitoring switch false."""
    arguments = ["test", "-no-color", f"-filter={TEST_FILE}"]
    if not monitoring:
        arguments.append("-var=monitoring_enabled=false")
    return stack.terraform(arguments, directory, {"TF_PLUGIN_CACHE_DIR": str(cache)})


def refusal(code: int, text: str, image: str) -> str | None:
    """None when the run failed with the precondition message that names the image, else why it did not."""
    if code == 0:
        return "terraform test passed"
    text = " ".join(text.split())
    if "Resource precondition failed" not in text or MESSAGE not in text:
        return "the failure is not the precondition of the repository resource"
    if f"Fix: {image}." not in text:
        return f"the message does not name {image} alone"
    return None


def declared_inputs() -> dict[str, bool]:
    """Read the real stack's files: no variable names another image list, the one file read is the image list, and the repositories come from its keys."""
    files = {path.name: path.read_text(encoding="utf-8") for path in sorted(PLATFORM.glob("*.tf"))}
    source = "\n".join(files.values())
    everything = source + "\n".join(path.read_text(encoding="utf-8") for path in sorted((PLATFORM / "tests").rglob("*")) if path.is_file())
    variables = re.findall(r'^variable "([a-z0-9_]+)"', source, re.M)
    calls = re.findall(r"\b(file|templatefile|filebase64|fileset|filesha256|filemd5|fileexists)\(([^)]*)\)", source)
    reader = '"${path.module}/../services/images.json"'
    return {
        "no variable of the stack names an image list, a manifest or a repository set": not [
            name for name in variables if re.search(r"manifest|repository|repositories|image_list|image_file|image_path|^images?$", name)
        ],
        "the stack reads one file, the image list, from a fixed path": calls == [("file", reader)],
        "the repositories are the keys of the image list less a switched-off group": "toset(keys(local.images))" in files["main.tf"]
        and files["registry.tf"].count('resource "aws_ecr_repository"') == 1
        and "for_each = local.ecr_repositories" in files["registry.tf"],
        "no file of the stack or its tests names an image manifest input": "image_manifest" not in everything,
    }


def cache_listing(folder: str | None) -> list[str]:
    """Every path under the caller's plugin cache, or an empty list when none is set or it does not exist."""
    if not folder or not Path(folder).is_dir():
        return []
    return sorted(str(path.relative_to(folder)) for path in Path(folder).rglob("*"))


def run(output: Path, plugin_directory: Path | None) -> dict[str, Any]:
    """Run every case in disposable copies and return the report."""
    cases: list[dict[str, Any]] = []
    caller_cache = os.environ.get("TF_PLUGIN_CACHE_DIR")
    cache_before = cache_listing(caller_cache)

    def record(name: str, problem: str | None) -> None:
        cases.append({"case": name, "passed": problem is None, "problem": problem})

    with tempfile.TemporaryDirectory(prefix="platform_groups.") as folder:
        scratch = Path(folder)
        cache = scratch / "plugin_cache"
        cache.mkdir()
        copy = copy_stack(scratch / "control", None, plugin_directory, cache)
        code, text = run_tests(copy, True, cache)
        record("the unchanged copy passes the test file", None if code == 0 and "Success!" in text else f"exit {code}: {text[-300:]}")
        for index, (label, image, value) in enumerate(BAD_GROUPS):
            copy = copy_stack(scratch / f"case_{index}", mutated(image, value), plugin_directory, cache)
            for monitoring in (True, False):
                code, text = run_tests(copy, monitoring, cache)
                record(f"{label} ({image}) with monitoring {'on' if monitoring else 'off'}", refusal(code, text, image))
    for name, ok in declared_inputs().items():
        record(name, None if ok else "the stack's files do not show it")
    changed = cache_listing(caller_cache) != cache_before
    record("the run leaves the caller's plugin cache (TF_PLUGIN_CACHE_DIR) unchanged", "the run wrote into the caller's plugin cache" if changed else None)
    report = {"passed": all(case["passed"] for case in cases), "cases": cases}
    output.mkdir(parents=True)
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output-directory", type=Path, required=True, help="A new folder for the report; it must not exist.")
    parser.add_argument(
        "--plugin-directory",
        type=Path,
        default=stack.PROVIDERS if stack.PROVIDERS.is_dir() else None,
        help="A folder with the AWS provider; defaults to infra/.terraform/providers.",
    )
    arguments = parser.parse_args()
    report = run(arguments.output_directory.expanduser().absolute(), arguments.plugin_directory)
    for case in report["cases"]:
        if not case["passed"]:
            sys.stdout.write(f"FAIL {case['case']}: {case['problem']}\n")
    passed = sum(case["passed"] for case in report["cases"])
    sys.stdout.write(f"event=platform_groups_e2e passed={report['passed']} cases={passed}/{len(report['cases'])}\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
