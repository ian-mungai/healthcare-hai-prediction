"""Record static pipeline graph, policy and CI dependency checks without starting a launcher."""

from __future__ import annotations

import ast
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from orchestration.graph import PIPELINE_CHAINS, requirements
from orchestration.network_policy import CLASSES, PROXY_IMAGE, load
from scripts.quality.ci_groups import plan

ROOT = Path(__file__).resolve().parents[2]
DEPENDENCIES = (
    "dbt/profiles.yml",
    "data_contracts/lakehouse/silver_publish.json",
    "scripts/lakehouse/pipeline_build.py",
    "scripts/lakehouse/silver_input.py",
    "scripts/lakehouse/silver_publish.py",
    "scripts/lakehouse/gold_maintenance.py",
)


def main() -> int:
    """Record static graph, network-policy and CI dependency results."""
    checks = {}
    inputs = {
        "config/quality/ci_groups.json",
        "services/orchestration/egress_proxy.json",
        "orchestration/network_policy.py",
        "orchestration/graph.py",
        "scripts/orchestration/run_pipeline_light.py",
    }
    inputs.update(DEPENDENCIES)
    for dag_id, chain in PIPELINE_CHAINS.items():
        name = f"dags/{dag_id}.py"
        inputs.add(name)
        tree = ast.parse((ROOT / name).read_text())
        literal = next(
            ast.literal_eval(node.value)
            for node in tree.body
            if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "WORK_STEPS" for target in node.targets)
        )
        expected, rules = requirements(dag_id)
        checks[f"graph_{dag_id}"] = {
            "ok": literal == chain
            and expected["catalog_release"] == frozenset(chain)
            and expected["release_run"] == frozenset({"final_gate"})
            and rules["release_run"] == "all_done"
        }
    for name in DEPENDENCIES:
        selected = plan("paths", "all", paths=[name])
        checks[f"ci_{name}"] = {"ok": "orchestration_checks" in selected["run"], "groups": selected["run"]}
    policy = load(ROOT / "services/orchestration/egress_proxy.json")
    checks["fixed_proxy_policy"] = {"ok": policy["image"] == PROXY_IMAGE and policy["classes"] == list(CLASSES)}
    templates = json.loads((ROOT / "config/orchestration/broker_templates.json").read_text())["templates"]
    checks["l2_deferred"] = {"ok": all("lineage" not in row for value in templates.values() for row in value["modes"].values())}
    folder = ROOT / "data/e2e/pipeline_light" / datetime.now(UTC).strftime("%Y%m%dT%H%M%S_%fZ")
    folder.mkdir(parents=True)
    report = {
        "scope": "static only",
        "checks": checks,
        "source_sha256": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in sorted(inputs)},
    }
    (folder / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    failed = [name for name, result in checks.items() if not result["ok"]]
    sys.stdout.write(json.dumps({"report": str(folder / "report.json"), "failed": failed}) + "\n")
    return int(bool(failed))


if __name__ == "__main__":
    sys.exit(main())
