"""Build source registry revision 3 from the archived revision 2 bytes (failure modes 716 to 723).

Applies the six owner decisions of Oct 8 2026 (STG-001, STG-007, STG-008, STG-009, STG-014, STG-016), with the owner's
answers of Oct 10 2026, as drafted in plans/registry_gaps_20261008/registry_diff.md. No control record is removed; every
replaced preserved_controls object is kept in historical_preserved_controls and every change adds a closeout decision
with its STG ID. The registry lock, the additions file and its lock, and the version catalog and its lock are rebound to
the new hash; revision 2 is catalogued second, after revision 1, so captures that recorded it keep verifying. Every run
rebuilds from the archive, so a rerun writes identical bytes. Run from the repository root:

    .venv/bin/python -m scripts.acquisition.one_off.registry_rev3.build_rev3          # check: no file changes
    .venv/bin/python -m scripts.acquisition.one_off.registry_rev3.build_rev3 --write
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from scripts.acquisition.registry_additions import additions_lock, canonical_json
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash, load_registry, require

CONFIG = REPO_ROOT / "config/acquisition"
ARCHIVE = REPO_ROOT / "data/acquisition_planning/acquisition_legacy_20261009/registry_rev2"
REV2 = "dbd6fa964a6001861f9701f6d2c785eccabb3ba314e586f860a462ab651bcad3"
FILES = {
    "registry": "source_registry.json",
    "lock": "source_registry_lock.json",
    "additions": "registry_additions.json",
    "additions_lock": "registry_additions_lock.json",
}
CATALOG = CONFIG / "registry_versions.json"
CATALOG_LOCK = CONFIG / "registry_versions.lock.json"
DECIDED = "2026-10-08"
SHEET = "plans/registry_gaps_20261008/decision_sheet.md"
C084_FIELDS = ["Bene_Race_Wht_Cnt", "Bene_Race_Black_Cnt", "Bene_Race_API_Cnt", "Bene_Race_Hspnc_Cnt", "Bene_Race_NatInd_Cnt", "Bene_Race_Othr_Cnt"]
C084_CHECKS = [
    "Bene_Race_Wht_Cnt: do not combine with Hispanic; the dictionary specifically labels this non-Hispanic White.",
    "Bene_Race_Black_Cnt: preserve the dictionary's non-Hispanic Black/African American definition; do not infer self-reported identity.",
    "Bene_Race_API_Cnt: do not split Asian and Pacific Islander counts; the public field combines them.",
    "Bene_Race_Hspnc_Cnt: treat as the CMS mutually categorized race/ethnicity classification, not an independently cross-tabulated ethnicity variable.",
    "Bene_Race_NatInd_Cnt: CCN 010001 is blank but 020001 is 236; a blank is not evidence of absence.",
    "Bene_Race_Othr_Cnt: do not rename this Unknown or calculate it as the remainder of the other five categories.",
    "Every race field: preserve suppressed and missing counts; use the provider's unique-beneficiary denominator; prior released CY only; assess "
    "subgroup missingness and retain as social context, with predictive usefulness Untested.",
]


def dump(value: Any, sort_keys: bool = False) -> str:
    """The registry files' own format: 2-space indent, ASCII escapes, one trailing newline."""
    return json.dumps(value, indent=2, sort_keys=sort_keys, ensure_ascii=True, allow_nan=False) + "\n"


def archived() -> dict[str, Any]:
    """Read revision 2 from the archive; its bytes must reproduce under the registry format and hash to revision 2 [716]."""
    documents = {}
    for key, name in FILES.items():
        raw = (ARCHIVE / name).read_text(encoding="utf-8")
        documents[key] = json.loads(raw)
        require(dump(documents[key]) == raw or dump(documents[key], sort_keys=True) == raw, f"archived {name} does not reproduce its bytes")
    require(canonical_hash(documents["registry"]) == REV2, "archived registry is not revision 2")
    return documents


def decide(control: dict[str, Any], entry: dict[str, Any], keep_history: bool = True) -> None:
    """Record one decision: append it, make it effective and keep the replaced text [718]."""
    control.setdefault("closeout_decisions", []).append(entry)
    control["effective_closeout_decision"] = entry
    if keep_history and "historical_preserved_controls" not in control:
        control["historical_preserved_controls"] = copy.deepcopy(control["preserved_controls"])


def apply_decisions(registry: dict[str, Any]) -> dict[str, Any]:
    """Return revision 3: the six decisions applied to a copy of revision 2."""
    new = copy.deepcopy(registry)
    controls = {control["id"]: control for control in new["measure_controls"]}

    # STG-001: C253 is BENE_DUAL_PCT alone.
    c253 = controls["C253"]
    decide(c253, {"decision": "redefined", "definition": "BENE_DUAL_PCT alone; the BENES_FFS_CNT population is not published", "issue": "STG-001"})
    c253["preserved_controls"]["current_exact_field"] = (
        "BENE_DUAL_PCT alone: the published share of Medicare fee-for-service beneficiaries eligible for Medicaid for at least one month in the "
        "year. The BENES_FFS_CNT population is not published in the 2014-2024 geographic variation files, was a weight and not the measure, "
        "and is not applied."
    )
    c253["preserved_controls"]["current_remaining_checks"].append(
        "Narrowed on 2026-10-08 (STG-001): the measure is bene_dual_pct alone; no BENES_FFS_CNT weight or eligible-population filter is "
        "applied. Earlier text kept in historical_preserved_controls."
    )

    # STG-007: C043 is closed as unavailable from public sources; its definition text stays (owner, Oct 10 2026).
    c043 = controls["C043"]
    decide(
        c043,
        {
            "decision": "closed_unavailable",
            "reason": "No unrestricted public source publishes direct-care worked hours per patient-day; occupational mix gives paid hours only",
            "issue": "STG-007",
        },
    )
    c043["preserved_controls"]["current_review_decision"] = "closed_unavailable_public_sources"
    c043["preserved_controls"]["current_remaining_checks"] = [
        "Closed by owner decision 2026-10-08 (STG-007): direct-care worked hours per patient-day are not published in any unrestricted public "
        "source reviewed. The occupational-mix surveys publish paid hours, which include leave and a broad inpatient and outpatient scope, so "
        "the occupational-mix measure rows carry no C043 value. A paid-hour control would be a new, separately reviewed measure, not C043. "
        "No substitute or model use approved. Earlier checks kept in historical_preserved_controls."
    ]
    c043["preserved_controls"]["hold_actions"] = []

    # STG-008: C001 and C040 lose their HHS part only; HCRIS, POS and IPPS stay (owner, Oct 10 2026).
    hhs_note = (
        "HHS part closed on 2026-10-08 (STG-008): the IPPS impact values cover every window; the HHS weekly series (Dec 29 2019 to Apr 21 "
        "2024) covers only the 2021 to 2024 windows as of the window start. The HHS columns stay staged in int_hhs_capacity_weeks and are not "
        "mapped to this control."
    )
    for control_id, suffix in (("C001", "; HHS staffed capacity variants"), ("C040", "; six distinct HHS occupancy pairs")):
        control = controls[control_id]
        field = control["preserved_controls"]["current_exact_field"]
        require(field.endswith(suffix), f"{control_id} field changed since the draft")
        decide(
            control,
            {
                "decision": "redefined",
                "definition": field[: -len(suffix)],
                "reason": "HHS part closed; the registry never named the HHS fields",
                "issue": "STG-008",
            },
        )
        control["preserved_controls"]["current_exact_field"] = field[: -len(suffix)]
        control["preserved_controls"]["current_remaining_checks"].append(hhs_note)

    # STG-009: C084 stays at field level; its six children are retired and their checks move to C084.
    c084 = controls["C084"]
    field_text = (
        "Each published Bene_Race_*_Cnt field over Tot_Benes, kept at field level: " + ", ".join(C084_FIELDS[:-1]) + f" and {C084_FIELDS[-1]}. "
        "The six child controls are retired; the published field name identifies each race. No invented Unknown field."
    )
    decide(c084, {"decision": "redefined", "definition": field_text, "issue": "STG-009"})
    c084["preserved_controls"]["current_exact_field"] = field_text
    c084["preserved_controls"]["current_remaining_checks"].extend(C084_CHECKS)
    for number in range(1, 7):
        child = controls[f"C084.0{number}"]
        decide(child, {"decision": "retired", "reason": "C084 kept at field level; the published field name identifies each race", "issue": "STG-009"})
        child["preserved_controls"]["current_exact_field"] = None
        child["preserved_controls"]["current_review_decision"] = "retired"
        child["preserved_controls"]["current_remaining_checks"] = [
            "Retired by owner decision 2026-10-08 (STG-009): C084 is kept at field level. Checks moved to C084."
        ]
        child["preserved_controls"]["hold_actions"] = []

    # STG-014: E038 and E039 name their published Care Compare IDs; the older IDs are not aliased (STG-013).
    notes = {
        "E038": (
            "PSI_90",
            " Published Care Compare measure ID: PSI_90 (CMS Medicare PSI 90: Patient safety and adverse events composite), read from the "
            "complications and deaths table from the July 2021 release. The earlier ID PSI_90_SAFETY (Serious complications; releases to April "
            "2021) is not this control and is not aliased (STG-013). v2025 is the AHRQ definition; the algorithm version of each published row "
            "is the one named in its release notes and is recorded per release, not assumed.",
        ),
        "E039": (
            "PSI_13",
            " Published Care Compare measure ID: PSI_13 (Postoperative sepsis rate), read from the complications and deaths table from the July "
            "2021 release. The earlier ID PSI_13_POST_SEPSIS (Blood stream infection after surgery; releases to April 2021) is not this control "
            "and is not aliased (STG-013). Whether a published row is risk-adjusted or observed is read from its release notes per release, not "
            "assumed.",
        ),
    }
    for control_id, (published, note) in notes.items():
        control = controls[control_id]
        decide(control, {"decision": "published_id_named", "published_id": published, "issue": "STG-014"})
        control["preserved_controls"]["current_exact_field"] += note

    # STG-016: C288.total_performance takes the parent's field and decision; C288.payment_adjustment is retired; the parent's
    # stale clause goes (owner, Oct 10 2026).
    total = controls["C288.total_performance"]
    decide(
        total,
        {"decision": "exact_field_assigned", "field": "total_performance_score", "decision_value": "exclude_primary", "issue": "STG-016"},
        keep_history=False,
    )
    total["preserved_controls"]["current_exact_field"] = "total_performance_score"
    total["preserved_controls"]["current_review_decision"] = "exclude_primary"
    payment = controls["C288.payment_adjustment"]
    decide(payment, {"decision": "retired", "issue": "STG-016"}, keep_history=False)
    payment["preserved_controls"]["current_review_decision"] = "retired"
    payment["preserved_controls"]["current_remaining_checks"] = [
        "Retired by owner decision 2026-10-08 (STG-016): the guide's external validation uses HAC Reduction Program penalty status (C285, "
        "staged), so a VBP payment factor adds a table and a source for no planned use. The TPS files publish no payment adjustment factor."
    ]
    c288 = controls["C288"]
    clause = "; payment adjustment factor exact source column not yet inspected"
    require(c288["preserved_controls"]["current_exact_field"].endswith(clause), "C288 field changed since the draft")
    decide(
        c288,
        {"decision": "redefined", "definition": "Total Performance Score in hvbp_tps", "reason": "payment adjustment retired (STG-016)", "issue": "STG-016"},
    )
    c288["preserved_controls"]["current_exact_field"] = "Total Performance Score in hvbp_tps"

    changed = [
        "C001",
        "C040",
        "C043",
        "C084",
        *(f"C084.0{n}" for n in range(1, 7)),
        "C253",
        "C288",
        "C288.payment_adjustment",
        "C288.total_performance",
        "E038",
        "E039",
    ]
    closeout = new["closeout"]
    closeout["user_decisions"].append(
        {
            "amends": closeout["user_decisions"][-1]["amends"],
            "decision": f"Owner decisions {DECIDED} (registry gaps STG-001, STG-007, STG-008, STG-009, STG-014, STG-016).",
            "decision_date": DECIDED,
            "measures": changed,
            "model_eligible": False,
            "rule": f"Applied as drafted in plans/registry_gaps_20261008/registry_diff.md with the owner's answers of 2026-10-10; decisions in {SHEET}.",
        }
    )
    closeout["revision_3"] = {
        "parent_registry_sha256": REV2,
        "reason": "Records the six owner decisions of 2026-10-08; revision 2 is catalogued with its archived bytes so its receipts keep verifying.",
    }
    new["registry_revision"] = 3
    return new


def check_unchanged_shape(old: dict[str, Any], new: dict[str, Any]) -> None:
    """No control removed or reordered; only the named controls and the closeout differ [719]."""
    require([c["id"] for c in old["measure_controls"]] == [c["id"] for c in new["measure_controls"]], "control IDs or order changed")
    require(old["counts"] == new["counts"] and old["inputs"] == new["inputs"] and old["sources"] == new["sources"], "counts, inputs or sources changed")
    for before, after in zip(old["measure_controls"], new["measure_controls"], strict=True):
        if before != after:
            require(after.get("effective_closeout_decision", {}).get("issue", "").startswith("STG-"), f"{after['id']} changed without a decision")
            history = after.get("historical_preserved_controls")
            require(
                history is None or history == before["preserved_controls"] or history == before.get("historical_preserved_controls"), f"{after['id']} lost text"
            )


def build() -> dict[Path, str]:
    """Return every file revision 3 writes, keyed by path."""
    old = archived()
    registry = apply_decisions(old["registry"])
    check_unchanged_shape(old["registry"], registry)
    new_hash = canonical_hash(registry)
    lock = {**old["lock"], "registry_sha256": new_hash}
    additions = {**old["additions"], "base_registry_sha256": new_hash}
    catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
    rev2 = {
        "registry_sha256": REV2,
        "registry_path": str((ARCHIVE / FILES["registry"]).relative_to(REPO_ROOT)),
        "lock_path": str((ARCHIVE / FILES["lock"]).relative_to(REPO_ROOT)),
        "files": {
            key: {"path": str((ARCHIVE / name).relative_to(REPO_ROOT)), "sha256": hashlib.sha256((ARCHIVE / name).read_bytes()).hexdigest()}
            for key, name in FILES.items()
        },
    }
    # Revision 1 stays first [722]; revision 2 is appended once [716].
    catalog["legacy_versions"] = [item for item in catalog["legacy_versions"] if item["registry_sha256"] != REV2] + [rev2]
    catalog["current_registry_sha256"] = new_hash
    return {
        CONFIG / FILES["registry"]: dump(registry),
        CONFIG / FILES["lock"]: dump(lock),
        CONFIG / FILES["additions"]: dump(additions),
        CONFIG / FILES["additions_lock"]: canonical_json(additions_lock(additions, registry)),
        CATALOG: dump(catalog),
        CATALOG_LOCK: dump({"catalog_sha256": canonical_hash(catalog)}),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--write", action="store_true", help="write the files; without it, report which would change")
    args = parser.parse_args()
    outputs = build()
    changed = [path for path, text in outputs.items() if not path.exists() or path.read_text(encoding="utf-8") != text]
    for path in changed:
        sys.stdout.write(f"{'wrote' if args.write else 'would change'} {path.relative_to(REPO_ROOT)}\n")
        if args.write:
            path.write_text(outputs[path], encoding="utf-8")
    if args.write:
        current = load_registry()
        # Revision 2 still resolves through the catalog [716].
        require(canonical_hash(load_registry(expected_sha256=REV2)) == REV2, "revision 2 no longer resolves")
        sys.stdout.write(f"registry revision {current['registry_revision']} {canonical_hash(current)[:12]}; revision 2 resolves\n")
    elif not changed:
        sys.stdout.write("no file changes\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
