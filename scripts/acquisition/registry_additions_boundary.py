"""Run fixed registry-additions mutation probes in a fresh Python process.

The closed command interface selects a copied case, named mutations and a known call.
Changed validator source stays in that case; repository source is never rewritten.
Focused calls report crossing one require boundary, not whole-document acceptance.
"""

from __future__ import annotations

import argparse
import sys
from importlib.machinery import SourceFileLoader
from pathlib import Path

from scripts.acquisition import registry_additions as module

FILES = ("source_registry.json", "source_registry_lock.json", "registry_additions.json", "registry_additions_lock.json")

# Fixed substitutions are selected by name; callers never supply Python source.
SENSITIVITY_RULES = [
    ("placement_changed_owner", "validate_placements(additions, registry)", "(0, 0)"),
    ("unplaced_changed_evidence", "validate_placements(additions, registry)", "(0, 0)"),
    ("sm_impossible_approval_date", "date.fromisoformat(str(decided_on))", "str(decided_on)"),
    ("sm_impossible_retrieval_time", "timestamp = datetime.fromisoformat(retrieved)", "timestamp = datetime(2026, 1, 1, tzinfo=UTC)"),
    ("sm_zero_year_retrieval", "timestamp = datetime.fromisoformat(retrieved)", "timestamp = datetime(2026, 1, 1, tzinfo=UTC)"),
    ("sm_approval_incomplete", "isinstance(approval, dict) and set(approval) == APPROVAL_KEYS", "True"),
    ("sm_approval_without_decision", 'nonempty_text(approval["decision"])', "True"),
    ("sm_evidence_count_mismatch", "len(evidence) == len(ids)", "True"),
    ("sm_evidence_out_of_order", 'record["dataset_id"] == dataset_id', "True"),
    ("sm_evidence_empty_rule", 'nonempty_text(record["rule"])', "True"),
    ("sm_evidence_empty_detail", 'nonempty_text(record["detail"])', "True"),
    ("sm_evidence_malformed_record", 'isinstance(record, dict) and DATASET_EVIDENCE_KEYS <= set(record) <= DATASET_EVIDENCE_KEYS | {"acquired_via"}', "True"),
    ("sm_evidence_lists_required", "isinstance(ids, list) and isinstance(evidence, list)", "True"),
    ("sm_publisher_evidence_not_object", "isinstance(evidence, dict)", "True"),
    ("sm_publisher_evidence_extra_key", "PUBLISHER_EVIDENCE_KEYS <= keys <= PUBLISHER_EVIDENCE_KEYS | PUBLISHER_EVIDENCE_OPTIONAL_KEYS", "True"),
    ("sm_empty_evidence_note", '"evidence_note" not in evidence or nonempty_text(evidence["evidence_note"])', "True"),
    ("sm_approval_empty_location", 'nonempty_text(approval["recorded_in"])', "True"),
    ("sm_impossible_retrieval_date", "timestamp = datetime.fromisoformat(retrieved)", "timestamp = datetime(2026, 1, 1, tzinfo=UTC)"),
]
for field in ("page_title", "naming_field", "naming_text"):
    SENSITIVITY_RULES.append((f"sm_empty_{field}", "nonempty_text(evidence[key])", f"(key == {field!r} or nonempty_text(evidence[key]))"))


PROBE_REPLACEMENTS = {name: (needle, replacement) for name, needle, replacement in SENSITIVITY_RULES}
PROBE_REPLACEMENTS.update(
    {
        "placement_rule": ("validate_placements(additions, registry)", "(0, 0)"),
        "relaxed_zip": ("zip(ids, evidence, strict=True)", "zip(ids, evidence, strict=False)"),
        "broken_both_controls": ('nonempty_text(record["rule"])', "undefined_sensitivity_control"),
        "broken_invalid_only": ('nonempty_text(record["rule"])', '(nonempty_text(record["rule"]) or undefined_sensitivity_control)'),
    }
)
BOUNDARY_RULES = {
    "sm_approval_incomplete": "source_metadata",
    "sm_evidence_lists_required": "source_metadata",
    "sm_publisher_evidence_not_object": "publisher_evidence",
}
BOUNDARY_MESSAGES = {
    "sm_approval_incomplete": "complete approval block",
    "sm_evidence_lists_required": "needs dataset_ids and evidence lists",
    "sm_publisher_evidence_not_object": "needs publisher evidence",
}


class BoundaryAccepted(Exception):
    """Stop immediately after the selected require condition accepts its input."""


def load_probe(case: Path, mutations: list[str]) -> None:
    """Load only fixed substitutions of the repository validator into this child."""
    original = Path(module.__file__)
    source = original.read_text(encoding="utf-8")
    for name in mutations:
        needle, replacement = PROBE_REPLACEMENTS[name]
        if source.count(needle) != 1:
            raise RuntimeError(f"Mutation {name} must match exactly one validator condition")
        source = source.replace(needle, replacement, 1)
    # The standard loader executes the changed copy in the original module namespace,
    # preserving imports and globals used by the existing CLI and placement checker.
    changed = case / "registry_additions_probe.py"
    changed.write_text(source, encoding="utf-8")
    SourceFileLoader(module.__name__, str(changed)).exec_module(module)


def focused_boundary(case: Path, boundary: str, call: str) -> None:
    """Check CMS_GV and stop only at the declared guard; never create a lock."""
    original_require = module.require

    def focused_require(condition: bool, message: str) -> None:
        original_require(condition, message)
        if BOUNDARY_MESSAGES[boundary] in message:
            raise BoundaryAccepted()

    module.require = focused_require
    try:
        registry = module.load_registry(case / FILES[0], case / FILES[1])
        additions = module.read_json(case / FILES[2])
        sources = module.index_records(registry["sources"], "source_id", "source")
        target = next(item for item in additions["source_metadata"] if item["source_id"] == "CMS_GV")
        if call == "publisher_evidence":
            module.validate_publisher_evidence(target["publisher_evidence"], "CMS_GV", sources["CMS_GV"])
        else:
            module.validate_source_metadata([target], sources)
    except BoundaryAccepted:
        sys.stdout.write("boundary_accepted:" + boundary + "\n")
    except module.RegistryError as error:
        sys.stderr.write("error: " + str(error) + "\n")
        raise SystemExit(2) from error
    else:
        raise RuntimeError("Named validation boundary was not reached")


def main() -> None:
    """Parse the closed probe interface and run a known CLI or guard call."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", required=True, type=Path)
    parser.add_argument("--call", required=True, choices=("write_lock", "check_lock", "source_metadata", "publisher_evidence"))
    parser.add_argument("--boundary", choices=tuple(BOUNDARY_RULES))
    parser.add_argument("--mutation", action="append", choices=tuple(PROBE_REPLACEMENTS), default=[])
    args = parser.parse_args()
    if args.boundary is None:
        if args.call not in ("write_lock", "check_lock"):
            parser.error("A focused call needs its named boundary")
    elif BOUNDARY_RULES[args.boundary] != args.call:
        parser.error("The named boundary does not match the selected call")
    load_probe(args.case, args.mutation)
    if args.boundary is not None:
        focused_boundary(args.case, args.boundary, args.call)
        return
    argv = [module.__name__]
    for flag, file_name in zip(("--registry", "--lock", "--additions", "--additions-lock"), FILES, strict=True):
        argv.extend((flag, str(args.case / file_name)))
    if args.call == "write_lock":
        argv.append("--write-lock")
    sys.argv = argv
    module.main()


if __name__ == "__main__":
    main()
