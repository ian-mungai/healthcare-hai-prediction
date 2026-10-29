"""End-to-end run of the bridge reference and recipe check: committed files pass and each damaged copy fails as expected (failure modes 800 to 803).

Run from the repository root:

    .venv/bin/python -m scripts.lakehouse.run_bridge_references_e2e [--output-directory DIR]

Every case copies the bridge seeds and the recipes to a scratch folder, damages one thing and runs ``bridge_references.check`` on the copy. The artifact
(``report.json`` in the output folder, default ``data/e2e/bridge_references/<UTC time>/``) lists each case with the messages that matched.

The run requires the gold contract. When the contract is absent the report names the skipped checks, the status is ``fail`` and no case counts as a pass;
the case ``absent_contract_does_not_pass`` shows the same for the reference check itself.
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import shutil
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import bridge_references as refs

Rows = list[dict[str, str]]


def load(path: Path) -> tuple[list[str], Rows]:
    """Return a CSV file's header and rows."""
    return refs.read_table(path)


def save(path: Path, header: list[str], rows: Rows) -> None:
    """Write a CSV file with Unix line endings."""
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def edit(path: Path, change: Callable[[Rows], Rows]) -> None:
    """Rewrite a CSV file's rows with a change."""
    header, rows = load(path)
    save(path, header, change(rows))


def blank(column: str, control: str, rule: str | None = None) -> Callable[[Rows], Rows]:
    """Return a change that blanks a column on every row of a control (and rule)."""

    def change(rows: Rows) -> Rows:
        for row in rows:
            if row["measure_control"] == control and rule in (None, row["rule"]):
                row[column] = ""
        return rows

    return change


def drop(control: str, rule: str, role: str | None = None) -> Callable[[Rows], Rows]:
    """Return a change that removes the rows of a control and rule (only one role when named)."""
    return lambda rows: [row for row in rows if not (row["measure_control"] == control and row["rule"] == rule and role in (None, row["role"]))]


def repeat_first(control: str, role: str) -> Callable[[Rows], Rows]:
    """Return a change that repeats the first row of a control and role."""

    def change(rows: Rows) -> Rows:
        index = next(i for i, row in enumerate(rows) if row["measure_control"] == control and row["role"] == role)
        return [*rows[: index + 1], dict(rows[index]), *rows[index + 1 :]]

    return change


def rekey(file: str, fact: str, control: str, old: str, new: str) -> Callable[[Rows], Rows]:
    """Return a change that sets the measure key of the rows of a fact and control that hold ``old`` to ``new``; it must change at least one row."""

    def change(rows: Rows) -> Rows:
        hits = [row for row in rows if row["fact"] == fact and row["measure_control"] == control and row["measure_key"] == old]
        if not hits:
            raise refs.ReferenceInputError(f"{file}: no {fact} row of {control} holds measure key {old!r}")
        for row in hits:
            row["measure_key"] = new
        return rows

    return change


def first_value(column: str, value: str) -> Callable[[Rows], Rows]:
    """Return a change that sets a column of the first row to a value."""

    def change(rows: Rows) -> Rows:
        rows[0][column] = value
        return rows

    return change


def append_row(row: dict[str, str]) -> Callable[[Rows], Rows]:
    """Return a change that adds a row at the end."""
    return lambda rows: [*rows, dict(row)]


def remove_where(column: str, value: str) -> Callable[[Rows], Rows]:
    """Return a change that removes the rows whose column holds the value; it must remove at least one."""

    def change(rows: Rows) -> Rows:
        kept = [row for row in rows if row[column] != value]
        if len(kept) == len(rows):
            raise refs.ReferenceInputError(f"no row holds {column} {value!r}")
        return kept

    return change


def passes(problems: list[str]) -> bool:
    """Return whether a reference check is a pass: no problem at all, so a skipped check is never a pass."""
    return not problems


def cases(seeds: Path, recipes: Path) -> list[tuple[str, Callable[[], None], str]]:
    """Return the damaged-copy cases: a name, the change applied to the scratch copy and the message the check must give."""

    def recipe(name: str, change: Callable[[Rows], Rows]) -> Callable[[], None]:
        return lambda: edit(recipes / f"{name}_recipe.csv", change)

    def invented_key() -> None:
        def change(rows: Rows) -> Rows:
            rows[0]["measure_key"] = "NOT_A_PLACES_MEASURE"
            return rows

        edit(seeds / "county_health_measures_bridge.csv", change)

    def key(file: str, fact: str, control: str, old: str, new: str) -> Callable[[], None]:
        return lambda: edit(seeds / file, rekey(file, fact, control, old, new))

    def empty_file() -> None:
        (recipes / "impact_measures_recipe.csv").write_text("")

    def header_only() -> None:
        header, _ = load(recipes / "cost_report_measures_recipe.csv")
        save(recipes / "cost_report_measures_recipe.csv", header, [])

    return [
        ("group_deleted", recipe("mup_measures", drop("C117", "drg_share")), "group ('C117', '49', 'drg_share') of the inventory is missing"),
        ("empty_file", empty_file, "impact_measures_recipe.csv: has no rows"),
        ("header_only_file", header_only, "cost_report_measures_recipe.csv: has no rows"),
        ("missing_mmd_value", recipe("mmd_conditions", drop("C258.01", "prevalence", "value")), "lacks roles ['value']"),
        ("blank_geography_level", recipe("mmd_conditions", blank("geography_level", "C258.03")), "geography_level is blank"),
        ("blank_source_status", recipe("occmix_measures", blank("source_status", "C030")), "source_status is blank"),
        ("blank_field_mapping", recipe("hhs_onc_measures", blank("field_mapping", "C260")), "field_mapping is blank"),
        ("blank_shortage_suppression", recipe("shortage_measures", blank("suppression", "C249")), "no row carries suppression"),
        ("blank_onc_suppression", recipe("hhs_onc_measures", blank("suppression", "C071")), "no row carries suppression"),
        ("blank_hhs_period", recipe("hhs_onc_measures", blank("period", "C260")), "no row carries period"),
        ("extra_input", recipe("cost_report_measures", repeat_first("C023", "numerator")), "the inventory says"),
        ("invented_long_key", invented_key, "no authoritative source supports measure key 'NOT_A_PLACES_MEASURE'"),
        (
            "places_unrelated_word",
            key("county_health_measures_bridge.csv", "fct_places_county_value", "C203", "DIABETES", "county"),
            "C203: no authoritative source supports measure key 'county' of fct_places_county_value: not an exact member of the PLACES domain",
        ),
        (
            "drg_unrelated_word",
            key("mup_measures_bridge.csv", "fct_medicare_inpatient_drg", "C117", "870", "year"),
            "C117: no authoritative source supports measure key 'year' of fct_medicare_inpatient_drg: not an exact member of the DRG domain",
        ),
        (
            "svi_identifier_key",
            key("acs_svi_measures_bridge.csv", "fct_svi_county_value", "C177", "ep_unemp", "county"),
            "C177: no authoritative source supports measure key 'county' of fct_svi_county_value: 'county' identifies a row of the SVI source",
        ),
        (
            "gv_identifier_key",
            key("county_health_measures_bridge.csv", "fct_gv_county_value", "C252", "ma_prtcptn_rate", "year"),
            "C252: no authoritative source supports measure key 'year' of fct_gv_county_value: 'year' identifies a row of the GV source",
        ),
        (
            "svi_case_changed",
            key("acs_svi_measures_bridge.csv", "fct_svi_county_value", "C177", "ep_unemp", "EP_UNEMP"),
            "measure key 'EP_UNEMP' of fct_svi_county_value: not an exact member of the SVI domain; it differs only in case from 'ep_unemp'",
        ),
        (
            "timely_invented_measure_id",
            key("registry_measure_sources_bridge.csv", "fct_timely_effective_care_window", "C141", "EDV", "ED_99"),
            "C141: no authoritative source supports measure key 'ED_99' of fct_timely_effective_care_window: not an exact member of the TIMELY domain",
        ),
        (
            "complications_case_changed",
            key("validation_measures_bridge.csv", "fct_complications_deaths_window", "C291", "MORT_30_AMI", "mort_30_ami"),
            "measure key 'mort_30_ami' of fct_complications_deaths_window: not an exact member of the COMPLICATIONS domain; it differs only in case",
        ),
        (
            "maternal_unrelated_word",
            key("registry_measure_sources_bridge.csv", "fct_maternal_health_window", "C167", "SM_7", "county"),
            "C167: no authoritative source supports measure key 'county' of fct_maternal_health_window: not an exact member of the MATERNAL domain",
        ),
        (
            "mmd_label_case_changed",
            key("mmd_conditions_bridge.csv", "fct_mmd_prevalence", "C258.03", "Anemia", "ANEMIA"),
            "measure key 'ANEMIA' of fct_mmd_prevalence: not an exact member of the MMD domain; it differs only in case from 'Anemia'",
        ),
        (
            "reference_drift",
            lambda: edit(seeds / "registry_controls.csv", lambda rows: rows[1:]),
            "registry_controls.csv differs from what the authoritative files generate",
        ),
        (
            "family_drift",
            lambda: edit(seeds / "registry_controls.csv", first_value("family", "Outcome")),
            "registry_controls.csv differs from what the authoritative files generate",
        ),
        (
            "closeout_drift",
            lambda: edit(seeds / "registry_controls.csv", first_value("closeout_decision", "dropped")),
            "registry_controls.csv differs from what the authoritative files generate",
        ),
        (
            "family_supplement_row_removed",
            lambda: edit(seeds / refs.FAMILY_SUPPLEMENT, remove_where("measure_control", "C285")),
            "control C285 has no source family and no row in registry_family_supplement.csv",
        ),
        (
            "family_supplement_for_sourced_control",
            lambda: edit(seeds / refs.FAMILY_SUPPLEMENT, append_row({"measure_control": "C001", "family": "Structure"})),
            "control C001 has a source family; remove it from the supplement",
        ),
        (
            "family_supplement_control_unknown",
            lambda: edit(seeds / refs.FAMILY_SUPPLEMENT, append_row({"measure_control": "C999", "family": "Structure"})),
            "control C999 is not in the registry",
        ),
        (
            "family_supplement_control_twice",
            lambda: edit(seeds / refs.FAMILY_SUPPLEMENT, lambda rows: [*rows, dict(rows[0])]),
            "control C285 appears twice",
        ),
        (
            "family_supplement_family_invented",
            lambda: edit(seeds / refs.FAMILY_SUPPLEMENT, first_value("family", "Mixed")),
            "control C285 has family 'Mixed'",
        ),
        (
            "closeout_spelling_not_mapped",
            lambda: edit(seeds / refs.CLOSEOUT_SPELLINGS, remove_where("raw_decision", "retired")),
            "has closeout spelling 'retired', which registry_closeout_spellings.csv does not map",
        ),
        (
            "closeout_spelling_twice",
            lambda: edit(seeds / refs.CLOSEOUT_SPELLINGS, lambda rows: [*rows, dict(rows[0])]),
            "spelling 'dropped' appears twice",
        ),
        (
            "closeout_value_invented",
            lambda: edit(seeds / refs.CLOSEOUT_SPELLINGS, first_value("closeout_decision", "closed")),
            "spelling 'dropped' maps to 'closed'",
        ),
        (
            "inventory_group_removed",
            lambda: edit(recipes / "recipe_groups.csv", lambda rows: [row for row in rows if row["measure_control"] != "C250"]),
            "is not in the inventory",
        ),
    ]


def absent_contract_case() -> dict[str, Any]:
    """Show that a run without the contract is never a pass: the strict check stops and the optional check reports the skipped checks."""
    missing = Path(tempfile.gettempdir()) / "bridge_references_absent_contract" / "gold_contract.json"
    try:
        refs.check(missing, False)
        strict_stops = False
    except refs.ReferenceInputError:
        strict_stops = True
    problems = refs.check(missing, True)
    skipped = [problem for problem in problems if refs.SKIPPED_MARK in problem]
    return {
        "case": "absent_contract_does_not_pass",
        "strict_check_stops": strict_stops,
        "skipped": skipped,
        "is_pass": passes(problems),
        "ok": strict_stops and bool(skipped) and not passes(problems),
    }


def registry_damage_cases() -> list[dict[str, Any]]:
    """Return the cases that damage the registry in memory (the registry is one fixed file): each must give its message and no family or decision."""
    registry = json.loads(refs.REGISTRY.read_text())
    additions = json.loads(refs.ADDITIONS.read_text())
    supplement = refs.read_rows(refs.GOLD_SEEDS / refs.FAMILY_SUPPLEMENT)
    spellings = refs.read_rows(refs.GOLD_SEEDS / refs.CLOSEOUT_SPELLINGS)

    def first_sourced_control(damaged: dict[str, Any]) -> dict[str, Any]:
        return next(control for control in damaged["measure_controls"] if control["id"] == "C001")

    def two_domains(damaged: dict[str, Any]) -> None:
        sources = {source["source_id"]: source for source in damaged["sources"]}
        source = sources[first_sourced_control(damaged)["source_ids"][0]]
        own = {refs.domain_of(family["dimension"]) for family in damaged["source_families"] if family["id"] in source["original_source_family_ids"]}
        other = next(family["id"] for family in damaged["source_families"] if refs.domain_of(family["dimension"]) not in own)
        source["original_source_family_ids"] = [*source["original_source_family_ids"], other]

    def unmapped_dimension(damaged: dict[str, Any]) -> None:
        damaged["source_families"][0]["dimension"] = "Quality: a new dimension"

    def unknown_source(damaged: dict[str, Any]) -> None:
        control = first_sourced_control(damaged)
        control["source_ids"] = [*control["source_ids"], "NOT_A_SOURCE"]

    def new_spelling(damaged: dict[str, Any]) -> None:
        control = next(control for control in damaged["measure_controls"] if control.get("effective_closeout_decision"))
        control["effective_closeout_decision"]["decision"] = "closed for a new reason"

    damage: list[tuple[str, Callable[[dict[str, Any]], None], str]] = [
        ("registry_control_in_two_domains", two_domains, "control C001 resolves to the domains"),
        ("registry_dimension_without_domain", unmapped_dimension, "has dimension 'Quality: a new dimension', which maps to no domain"),
        ("registry_control_names_unknown_source", unknown_source, "control C001 names source NOT_A_SOURCE, which is not in the registry"),
        ("registry_new_closeout_spelling", new_spelling, "has closeout spelling 'closed for a new reason', which registry_closeout_spellings.csv does not map"),
    ]
    results: list[dict[str, Any]] = []
    for name, change, expected in damage:
        damaged = copy.deepcopy(registry)
        change(damaged)
        records = refs.registry_records(damaged, additions)
        problems = [*refs.control_families(damaged, records, supplement)[1], *refs.closeout_decisions(records, spellings)[1]]
        matched = [problem for problem in problems if expected in problem]
        results.append({"case": name, "expected": expected, "matched": matched[:3], "problems": len(problems), "ok": bool(matched)})
    return results


def run(output: Path) -> dict[str, Any]:
    """Run every case on scratch copies and return the report; without the contract no case runs and the status is fail."""
    results: list[dict[str, Any]] = []
    contract_present = refs.CONTRACT.exists()
    if contract_present:
        with tempfile.TemporaryDirectory() as scratch:
            seeds, recipes = Path(scratch) / "seeds", Path(scratch) / "recipes"
            base = refs.check(refs.CONTRACT, False)
            results.append({"case": "committed_files", "problems": base, "ok": passes(base)})
            for name, damage, expected in cases(seeds, recipes):
                for folder, source in ((seeds, refs.GOLD_SEEDS), (recipes, refs.RECIPES)):
                    shutil.rmtree(folder, ignore_errors=True)
                    shutil.copytree(source, folder)
                damage()
                problems = refs.check(refs.CONTRACT, False, seeds, recipes)
                matched = [problem for problem in problems if expected in problem]
                results.append({"case": name, "expected": expected, "matched": matched[:3], "problems": len(problems), "ok": bool(matched)})
    else:
        skipped = f"every case was skipped: {refs.CONTRACT} is absent, so no check that needs the gold contract ran"
        results.append({"case": "committed_files", "problems": [skipped], "ok": False})
    results.extend(registry_damage_cases())
    results.append(absent_contract_case())
    report: dict[str, Any] = {
        "written_utc": datetime.now(UTC).isoformat(),
        "contract_present": contract_present,
        "cases": results,
        "status": "pass" if contract_present and all(r["ok"] for r in results) else "fail",
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> int:
    """Run the cases, write the artifact and fail when a case does not behave as expected."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output-directory", type=Path, default=refs.REPO_ROOT / "data/e2e/bridge_references" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))
    args = parser.parse_args()
    report = run(args.output_directory)
    failed = [result["case"] for result in report["cases"] if not result["ok"]]
    sys.stdout.write(f"bridge references e2e: {report['status']}; artifact {args.output_directory / 'report.json'}\n")
    if failed:
        sys.stderr.write(f"bridge references e2e: failed cases {failed}\n")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
