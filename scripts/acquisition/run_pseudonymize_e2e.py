"""Repeatable synthetic verification of the keyed pseudonymization and its admission contract: no real data, no AWS call, no real key.

Every input is generated here from invented names. The key reaches the transformation only through its key-reader seam, as text built from a label.
"""

from __future__ import annotations

import argparse
import base64
import copy
import csv
import hashlib
import json
import shutil
import socket
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any
from unittest.mock import patch

from botocore.exceptions import ClientError, EndpointConnectionError

from scripts.acquisition import pseudonymize as module
from scripts.acquisition.cli_tools import run_argv
from scripts.acquisition.pseudonymize import PseudonymizationError
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import REPO_ROOT, canonical_hash

OWNER_HEADERS = [
    "ENROLLMENT ID",
    "ASSOCIATE ID - OWNER",
    "TYPE - OWNER",
    "FIRST NAME - OWNER",
    "MIDDLE NAME - OWNER",
    "LAST NAME - OWNER",
    "TITLE - OWNER",
    "ORGANIZATION NAME - OWNER",
    "ADDRESS LINE 1 - OWNER",
    "PERCENTAGE OWNERSHIP",
]
OWNER_ROWS = [
    ["E001", "0123456789", "I", "Zelda", "Q", "Quimby", "Owner Alpha", "", "11 Nowhere Lane", "25"],
    ["E002", " 01234 56789", "I", "ZELDA", "", "quimby", "Director", "", "22 Elsewhere Road", "50"],
    ["E003", "", "I", "José", "", "Núñez", "Officer", "", "", "10"],
    ["E004", "N/A", "I", "jose", "", "NUNEZ Jr.", "", "", "", "10"],
    ["E005", "", "O", "", "", "", "", "Zelda Quimby Holdings LLC", "", "100"],
    ["E006", "", "O", "", "", "", "", "Plain Industries Inc", "", "100"],
    ["E007", "", "I", "", "", "Lonelyname", "", "", "", "5"],
    ["E008", "0999999999", "I", "Wally", "", "O'Brien-Sample", "", "", "33 Nowhere Way", "5"],
    ["E009", "0999999999", "I", "Wally", "", "Obrien Sample", "", "", "", "5"],
    ["E010", "0111111111", "I", "Wally", "", "O’Brien Sample", "", "", "", "5"],
]
DIRECT_VALUES = [
    "zelda",
    "quimby",
    "josé",
    "núñez",
    "nunez",
    "wally",
    "brien",
    "lonelyname",
    "0123456789",
    "01234 56789",
    "0999999999",
    "0111111111",
    "owner alpha",
    "director",
    "officer",
    "nowhere lane",
    "elsewhere road",
    "nowhere way",
]
OWNER_POLICY: dict[str, Any] = {
    "policy_id": "synthetic_owner_rows",
    "policy_version": 1,
    "source_id": "SYNTHETIC_OWNERS",
    "normalization_version": "norm_v1",
    "headers": OWNER_HEADERS,
    "treatments": {
        "ENROLLMENT ID": {"treatment": "keep"},
        "ASSOCIATE ID - OWNER": {"treatment": "drop"},
        "TYPE - OWNER": {"treatment": "keep"},
        "FIRST NAME - OWNER": {"treatment": "drop"},
        "MIDDLE NAME - OWNER": {"treatment": "drop"},
        "LAST NAME - OWNER": {"treatment": "drop"},
        "TITLE - OWNER": {"treatment": "drop"},
        "ORGANIZATION NAME - OWNER": {"treatment": "scrub_names"},
        "ADDRESS LINE 1 - OWNER": {"treatment": "drop"},
        "PERCENTAGE OWNERSHIP": {"treatment": "keep"},
    },
    "pseudonyms": [
        {"output": "owner_person_pseudonym", "kind": "id", "namespace": "synthetic_associate_id", "columns": ["ASSOCIATE ID - OWNER"]},
        {
            "output": "owner_name_match_pseudonym",
            "kind": "name",
            "namespace": "person_name",
            "columns": ["FIRST NAME - OWNER", "LAST NAME - OWNER"],
            "middle_column": "MIDDLE NAME - OWNER",
        },
    ],
    "max_bytes": 1024**2,
    "min_rows": 1,
}
INDIVIDUAL = {"column": "LICEE_TOC", "equals": "Investor - Individual"}
FACILITY_HEADERS = ["FAC_NO", "LICEE_TOC", "FAC_PAR_CORP_NAME", "FAC_PAR_CORP_CITY", "BEDS"]
FACILITY_ROWS = [
    ["100", "Investor - Individual", "Nova Sample", "Sampletown", "50"],
    ["101", "Investor - Corporation", "Acme Sample Corp", "Orgville", "60"],
    ["102", "Investor - Individual", "NOVA  sample", "Sampletown", "70"],
]
FACILITY_POLICY: dict[str, Any] = {
    "policy_id": "synthetic_facility_rows",
    "policy_version": 1,
    "source_id": "SYNTHETIC_FACILITIES",
    "normalization_version": "norm_v1",
    "headers": FACILITY_HEADERS,
    "treatments": {
        "FAC_NO": {"treatment": "keep"},
        "LICEE_TOC": {"treatment": "keep"},
        "FAC_PAR_CORP_NAME": {"treatment": "keep", "blank_when": INDIVIDUAL},
        "FAC_PAR_CORP_CITY": {"treatment": "keep", "blank_when": INDIVIDUAL},
        "BEDS": {"treatment": "keep"},
    },
    "pseudonyms": [
        {"output": "individual_owner_name_pseudonym", "kind": "name", "namespace": "person_name", "columns": ["FAC_PAR_CORP_NAME"], "when": INDIVIDUAL}
    ],
    "max_bytes": 1024**2,
    "min_rows": 1,
}
NO_NETWORK_MESSAGE = "network access attempted"


def key_text(label: str) -> str:
    """A synthetic key as the 32 bytes of a hash of the label, in URL-safe base64."""
    return base64.urlsafe_b64encode(hashlib.sha256(label.encode()).digest()).decode()


KEYS = {"v1": key_text("synthetic key one"), "v2": key_text("synthetic key two")}
CHANGED_V1 = key_text("synthetic key one, changed")


def reader_for(keys: dict[str, str]) -> module.KeyReader:
    """A key reader that serves the synthetic keys and returns nothing for another version."""
    return partial(dict.get, keys)


def ensure(condition: bool, message: str) -> None:
    """Fail the case with the message when the condition is false."""
    if not condition:
        raise AssertionError(message)


def expect_error(code: str, call: Callable[[], Any]) -> None:
    """The call must raise the module error with exactly this code."""
    try:
        call()
    except PseudonymizationError as error:
        ensure(str(error) == code, f"expected {code}, observed {error}")
        return
    raise AssertionError(f"expected {code}, nothing raised")


def write_csv(path: Path, headers: list[str], rows: list[list[str]]) -> Path:
    """Write a synthetic CSV with LF line endings."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(headers)
        writer.writerows(rows)
    return path


def table(directory: Path) -> list[dict[str, str]]:
    """The rows of a derivative as dictionaries."""
    with (directory / module.DATA_NAME).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle, strict=True))


def tree(directory: Path) -> dict[str, tuple[str, int]]:
    """Every file under the directory with its hash and size."""
    return {str(p.relative_to(directory)): fingerprint(p) for p in sorted(directory.rglob("*")) if p.is_file()}


def reseal(directory: Path, edit: Callable[[str], str]) -> None:
    """Edit a copied derivative and bind its contract to the edited bytes, to reach checks beyond the hash check."""
    data = directory / module.DATA_NAME
    data.write_text(edit(data.read_text()))
    contract = json.loads((directory / module.CONTRACT_NAME).read_text())
    contract["output"]["sha256"], contract["output"]["byte_count"] = fingerprint(data)
    contract.pop("contract_sha256")
    contract["contract_sha256"] = canonical_hash(contract)
    (directory / module.CONTRACT_NAME).write_text(json.dumps(contract))


def refuse_network(*_: Any, **__: Any) -> None:
    """Replace socket connection so a case that reaches a network fails."""
    raise AssertionError(NO_NETWORK_MESSAGE)


class StandInParameters:
    """A Parameter Store client stand-in with the one call the key reader makes."""

    def __init__(self, values: dict[str, str], failure: Exception | None = None) -> None:
        self.values, self.failure = values, failure
        self.calls: list[tuple[str, bool]] = []

    def get_parameter(self, Name: str, WithDecryption: bool) -> dict:
        """Record the request, then fail, miss or answer as configured."""
        self.calls.append((Name, WithDecryption))
        if self.failure is not None:
            raise self.failure
        if Name not in self.values:
            raise ClientError({"Error": {"Code": "ParameterNotFound", "Message": "missing"}}, "GetParameter")
        return {"Parameter": {"Value": self.values[Name]}}


class Run:
    """One verification run: the artifact folder, its fixed inputs and the recorded cases."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.cases: list[dict] = []
        self.original = write_csv(root / "inputs" / "owners.csv", OWNER_HEADERS, OWNER_ROWS)
        self.facilities = write_csv(root / "inputs" / "facilities.csv", FACILITY_HEADERS, FACILITY_ROWS)
        self.original_print = fingerprint(self.original)
        self.policy_path = root / "inputs" / "owner_policy.json"
        write_once(self.policy_path, encoded_json(OWNER_POLICY))
        self.facility_policy_path = root / "inputs" / "facility_policy.json"
        write_once(self.facility_policy_path, encoded_json(FACILITY_POLICY))

    def case_dir(self, name: str) -> Path:
        """A new folder for one case."""
        folder = self.root / "cases" / name
        folder.mkdir(parents=True)
        return folder

    def scenario(self, name: str, body: Callable[[], None]) -> None:
        """Run one case with sockets refused and record the outcome with the failing message."""
        try:
            with patch.object(socket.socket, "connect", refuse_network):
                body()
        except (AssertionError, PseudonymizationError, OSError, KeyError, ValueError) as error:
            self.cases.append({"case": name, "passed": False, "detail": f"{type(error).__name__}: {error}"})
        else:
            self.cases.append({"case": name, "passed": True})

    def make(self, name: str, version: str = "v1", keys: dict[str, str] | None = None, policy: dict | None = None, source: Path | None = None) -> dict:
        """Transform the owner file into a new folder of its own and return the contract."""
        folder = self.case_dir(name)
        return module.transform(source or self.original, folder / "derivative", policy or OWNER_POLICY, version, reader_for(keys or KEYS))

    def derivative(self, name: str, version: str = "v1") -> Path:
        """The derivative folder of a case that made one."""
        self.make(name, version)
        return self.root / "cases" / name / "derivative"

    def cli(self, *arguments: str) -> tuple[int, str]:
        """Run the command line in a child process and return its exit code and everything it printed."""
        result = run_argv([sys.executable, "-m", "scripts.acquisition.pseudonymize", *arguments], cwd=REPO_ROOT, timeout=60)
        return result.returncode, result.stdout + result.stderr


def check_preservation(run: Run) -> None:
    """Every input row gives one output row in order; the contract records both hashes, the counts and the policy."""
    contract = run.make("preservation")
    folder = run.root / "cases" / "preservation" / "derivative"
    rows = table(folder)
    ensure(len(rows) == len(OWNER_ROWS) == contract["input"]["row_count"] == contract["output"]["row_count"], "row count differs")
    ensure([r["ENROLLMENT ID"] for r in rows] == [r[0] for r in OWNER_ROWS], "row order or content differs")
    ensure([r["PERCENTAGE OWNERSHIP"] for r in rows] == [r[-1] for r in OWNER_ROWS], "kept cells differ")
    individuals = [r for r in rows if r["TYPE - OWNER"] == "I"]
    ensure(len(individuals) == 8, "individual-owner rows were not preserved")
    ensure(contract["input"]["sha256"] == run.original_print[0] and contract["input"]["byte_count"] == run.original_print[1], "input hash differs")
    ensure(fingerprint(folder / module.DATA_NAME)[0] == contract["output"]["sha256"], "output hash differs")
    ensure(contract["derivative_policy"]["policy_sha256"] == canonical_hash(OWNER_POLICY), "policy hash differs")
    ensure(contract["derivative_policy"]["classification"] == module.CLASSIFICATION and contract["key_version"] == "v1", "policy or key version differs")
    ensure(contract["model_eligible"] is False and contract["lookup_table_written"] is False, "contract holds differ")
    ensure(contract["name_pseudonyms_prove_identity"] is False and contract["pseudonymization_is_anonymization"] is False, "contract caveats differ")
    by_output = {p["output"]: p for p in contract["pseudonyms"]}
    ids, names = by_output["owner_person_pseudonym"], by_output["owner_name_match_pseudonym"]
    ensure((ids["written"], ids["withheld"], ids["distinct"], ids["match_strength"]) == (5, 5, 3, "authoritative_id"), f"id counts differ: {ids}")
    ensure((names["written"], names["withheld"], names["distinct"], names["match_strength"]) == (7, 3, 3, "name_match_only"), f"name counts differ: {names}")
    ensure(contract["scrubbed_cells"] == 1 and rows[4]["ORGANIZATION NAME - OWNER"] == module.REPLACEMENT, "organisation name was not scrubbed")
    ensure(rows[5]["ORGANIZATION NAME - OWNER"] == "Plain Industries Inc", "an unrelated organisation name changed")


def check_no_direct_identifiers(run: Run) -> None:
    """No original value, in any case, and no dropped column name appears in the derivative or its contract."""
    run.make("direct_identifiers")
    folder = run.root / "cases" / "direct_identifiers" / "derivative"
    text = "".join((folder / name).read_text().casefold() for name in (module.DATA_NAME, module.CONTRACT_NAME))
    leaked = [value for value in DIRECT_VALUES if value in text]
    ensure(not leaked, f"direct identifiers present: {leaked}")
    header = table(folder)[0].keys()
    ensure(
        not {"ASSOCIATE ID - OWNER", "FIRST NAME - OWNER", "LAST NAME - OWNER", "TITLE - OWNER", "ADDRESS LINE 1 - OWNER"} & set(header), "dropped column kept"
    )


def check_stability(run: Run) -> None:
    """The same input, key and version give the same bytes in a new folder, in another row order and through the Parameter Store reader."""
    first, second = run.make("stable_a"), run.make("stable_b")
    ensure(first == second, "contracts differ between runs")
    ensure(tree(run.root / "cases/stable_a/derivative") == tree(run.root / "cases/stable_b/derivative"), "bytes differ between runs")
    shuffled_rows = [*OWNER_ROWS[6:][::-1], *OWNER_ROWS[:6]]
    shuffled = write_csv(run.case_dir("stable_shuffled") / "owners.csv", OWNER_HEADERS, shuffled_rows)
    run.make("stable_shuffled_out", source=shuffled)
    base = {(r["ENROLLMENT ID"], r["owner_person_pseudonym"], r["owner_name_match_pseudonym"]) for r in table(run.root / "cases/stable_a/derivative")}
    again = {
        (r["ENROLLMENT ID"], r["owner_person_pseudonym"], r["owner_name_match_pseudonym"]) for r in table(run.root / "cases/stable_shuffled_out/derivative")
    }
    ensure(base == again, "pseudonyms depend on row order")
    stand_in = StandInParameters({module.PARAMETER_PREFIX + "v1": KEYS["v1"]})
    folder = run.case_dir("stable_parameter_store")
    through = module.transform(run.original, folder / "derivative", OWNER_POLICY, "v1", module.parameter_store_reader(stand_in))
    ensure(through == first and stand_in.calls == [(module.PARAMETER_PREFIX + "v1", True)], f"Parameter Store read differs: {stand_in.calls}")


def check_equal_people(run: Run) -> None:
    """The same identifier or the same normalized name gives one pseudonym; the two kinds differ; a shared name does not merge distinct identifiers."""
    rows = {r["ENROLLMENT ID"]: r for r in table(run.derivative("people"))}
    pid, name = "owner_person_pseudonym", "owner_name_match_pseudonym"
    ensure(rows["E001"][pid] == rows["E002"][pid] != "", "one identifier gave two pseudonyms")
    ensure(rows["E001"][name] == rows["E002"][name] != "", "case and spacing changed a name pseudonym")
    ensure(rows["E003"][name] == rows["E004"][name] != "", "accents and a suffix changed a name pseudonym")
    ensure(rows["E008"][name] == rows["E009"][name] == rows["E010"][name] != "", "punctuation changed a name pseudonym")
    ensure(rows["E008"][pid] == rows["E009"][pid] != rows["E010"][pid], "identifier pseudonyms do not separate two people with one name")
    ensure(rows["E001"][pid] != rows["E001"][name] and rows["E001"][name] != rows["E003"][name], "distinct inputs share a pseudonym")
    ensure(rows["E004"][pid] == "" and rows["E007"][name] == "" and rows["E005"][name] == "", "an unusable identity produced a pseudonym")
    ensure(all(v.startswith("v1:") and len(v) == 67 for r in rows.values() for v in (r[pid], r[name]) if v), "pseudonym format differs")


def check_key_versions(run: Run) -> None:
    """A second key version changes every pseudonym; a changed key under one version label cannot overwrite an earlier result."""
    one, two = table(run.derivative("version_one", "v1")), table(run.derivative("version_two", "v2"))
    for column in ("owner_person_pseudonym", "owner_name_match_pseudonym"):
        pairs = [(a[column], b[column]) for a, b in zip(one, two, strict=True)]
        ensure(all((a == b == "") or (a != "" and b != "" and a != b) for a, b in pairs), f"{column} did not change with the key")
        ensure(all(b.startswith("v2:") for _, b in pairs if b), "version prefix differs")
    contract_one = json.loads((run.root / "cases/version_one/derivative" / module.CONTRACT_NAME).read_text())
    contract_two = json.loads((run.root / "cases/version_two/derivative" / module.CONTRACT_NAME).read_text())
    ensure(module.comparable(contract_one, contract_two) == ["key_version", "key_check"], "comparability fields differ")
    ensure(module.comparable(contract_one, contract_one) == [], "one contract differs from itself")
    folder = run.case_dir("changed_key")
    module.transform(run.original, folder / "derivative", OWNER_POLICY, "v1", reader_for(KEYS))
    before = tree(folder / "derivative")
    expect_error(
        "key_changed_under_same_version", lambda: module.transform(run.original, folder / "derivative", OWNER_POLICY, "v1", reader_for({"v1": CHANGED_V1}))
    )
    ensure(tree(folder / "derivative") == before, "a changed key altered an earlier result")
    other = module.transform(run.original, folder / "elsewhere", OWNER_POLICY, "v1", reader_for({"v1": CHANGED_V1}))
    ensure(
        other["key_check"] != contract_one["key_check"] and other["output"]["sha256"] != contract_one["output"]["sha256"], "a changed key gave the same result"
    )


def check_idempotence(run: Run) -> None:
    """A rerun changes nothing; a conflicting folder is refused and left as found; an interrupted run leaves no partial result."""
    folder = run.case_dir("rerun")
    first = module.transform(run.original, folder / "derivative", OWNER_POLICY, "v1", reader_for(KEYS))
    before = tree(folder / "derivative")
    ensure(module.transform(run.original, folder / "derivative", OWNER_POLICY, "v1", reader_for(KEYS)) == first, "rerun contract differs")
    ensure(tree(folder / "derivative") == before and sorted(p.name for p in folder.iterdir()) == ["derivative"], "rerun changed state or left staging files")
    (folder / "derivative" / module.DATA_NAME).write_text("planted\n")
    planted = tree(folder / "derivative")
    expect_error("conflicting_output", lambda: module.transform(run.original, folder / "derivative", OWNER_POLICY, "v1", reader_for(KEYS)))
    ensure(tree(folder / "derivative") == planted and sorted(p.name for p in folder.iterdir()) == ["derivative"], "conflict handling changed state")
    crash = run.case_dir("interrupted")
    with patch.object(module.os, "rename", side_effect=OSError("disk gone")):
        try:
            module.transform(run.original, crash / "derivative", OWNER_POLICY, "v1", reader_for(KEYS))
        except OSError:
            pass
        else:
            raise AssertionError("the interrupted run did not fail")
    ensure(sorted(p.name for p in crash.iterdir()) == [], "an interrupted run left files behind")
    ensure(module.transform(run.original, crash / "derivative", OWNER_POLICY, "v1", reader_for(KEYS)) == first, "the rerun after an interruption differs")
    ensure(fingerprint(run.original) == run.original_print, "the original changed")


def check_key_errors(run: Run) -> None:
    """A missing, empty, malformed or wrong-size key and an invalid version raise a named error and write nothing."""
    cases = {
        "missing": ("key_missing", "v3", {}),
        "empty": ("key_missing", "v1", {"v1": "   "}),
        "not_base64": ("key_malformed", "v1", {"v1": "not base64 !!"}),
        "short": ("key_malformed", "v1", {"v1": base64.urlsafe_b64encode(b"short").decode()}),
        "long": ("key_malformed", "v1", {"v1": base64.urlsafe_b64encode(bytes(64)).decode()}),
        "non_ascii": ("key_malformed", "v1", {"v1": "\u00e9" * 44}),
        "version_zero": ("key_version_invalid", "v0", KEYS),
        "version_bare": ("key_version_invalid", "1", KEYS),
        "version_path": ("key_version_invalid", "../v1", KEYS),
    }
    for name, (code, version, keys) in cases.items():
        folder = run.case_dir("error_" + name)
        expect_error(code, partial(module.transform, run.original, folder / "derivative", OWNER_POLICY, version, reader_for(keys)))
        ensure(not (folder / "derivative").exists() and sorted(p.name for p in folder.iterdir()) == [], f"{name} left files behind")
    absent = StandInParameters({})
    expect_error("key_missing", lambda: module.transform(run.original, run.case_dir("absent") / "d", OWNER_POLICY, "v1", module.parameter_store_reader(absent)))
    denied = ClientError({"Error": {"Code": "AccessDeniedException", "Message": "denied"}}, "GetParameter")
    expect_error(
        "key_read_failed",
        lambda: module.transform(run.original, run.case_dir("denied") / "d", OWNER_POLICY, "v1", module.parameter_store_reader(StandInParameters({}, denied))),
    )
    unreachable = EndpointConnectionError(endpoint_url="https://stand-in.invalid")
    expect_error(
        "key_read_failed",
        lambda: module.transform(
            run.original, run.case_dir("unreachable") / "d", OWNER_POLICY, "v1", module.parameter_store_reader(StandInParameters({}, unreachable))
        ),
    )


def check_input_errors(run: Run) -> None:
    """Malformed input raises one named error each and writes nothing."""
    header = ",".join(OWNER_HEADERS)
    width = ",".join(["x"] * len(OWNER_HEADERS))
    payloads = {
        "schema_drift": (b"ENROLLMENT ID,NEW\n1,2\n", "schema_drift"),
        "row_width_mismatch": (f"{header}\n1,2\n".encode(), "row_width_mismatch"),
        "encoding_invalid": (b"\xff\xfe\x00bad", "encoding_invalid"),
        "csv_malformed": (f'{header}\n{width},"unfinished'.encode(), "csv_malformed"),
        "oversized_cell": (f"{header}\n{width.replace('x', 'y' * 5000, 1)}\n".encode(), "oversized_cell"),
        "row_count_below_minimum": (f"{header}\n".encode(), "row_count_below_minimum"),
        "empty_file": (b"", "input_size_limit"),
    }
    for name, (payload, code) in payloads.items():
        folder = run.case_dir("input_" + name)
        source = folder / "in.csv"
        source.write_bytes(payload)
        expect_error(code, partial(module.transform, source, folder / "derivative", OWNER_POLICY, "v1", reader_for(KEYS)))
        ensure(sorted(p.name for p in folder.iterdir()) == ["in.csv"], f"{name} left files behind")
    folder = run.case_dir("input_size_limit")
    expect_error("input_size_limit", lambda: module.transform(run.original, folder / "d", {**OWNER_POLICY, "max_bytes": 10}, "v1", reader_for(KEYS)))
    expect_error("input_hash_mismatch", lambda: module.transform(run.original, folder / "d", OWNER_POLICY, "v1", reader_for(KEYS), "0" * 64))
    expect_error("invalid_hash_pin", lambda: module.transform(run.original, folder / "d", OWNER_POLICY, "v1", reader_for(KEYS), "ABC"))
    ok = module.transform(run.original, folder / "pinned", OWNER_POLICY, "v1", reader_for(KEYS), run.original_print[0])
    ensure(ok["input"]["sha256"] == run.original_print[0], "a matching pin was refused")
    link = folder / "link.csv"
    link.symlink_to(run.original)
    expect_error("invalid_input_path", lambda: module.transform(link, folder / "d", OWNER_POLICY, "v1", reader_for(KEYS)))
    expect_error("input_inside_output", lambda: module.transform(run.original, run.original.parent, OWNER_POLICY, "v1", reader_for(KEYS)))
    expect_error("invalid_input_path", lambda: module.transform(folder / "missing.csv", folder / "d", OWNER_POLICY, "v1", reader_for(KEYS)))
    ensure(fingerprint(run.original) == run.original_print, "the original changed")


def check_policy_errors(run: Run) -> None:
    """A policy that is open, incomplete or unsafe is refused before any row is read."""

    def changed(edit: Callable[[dict], None]) -> dict:
        policy = copy.deepcopy(OWNER_POLICY)
        edit(policy)
        return policy

    def unlisted(policy: dict) -> None:
        del policy["treatments"]["TITLE - OWNER"]

    def source_kept(policy: dict) -> None:
        policy["treatments"]["LAST NAME - OWNER"] = {"treatment": "keep"}

    def duplicate_output(policy: dict) -> None:
        policy["pseudonyms"][1]["output"] = policy["pseudonyms"][0]["output"]

    def output_in_headers(policy: dict) -> None:
        policy["pseudonyms"][0]["output"] = "owner_person_pseudonym_x"
        policy["headers"].append("owner_person_pseudonym_x")
        policy["treatments"]["owner_person_pseudonym_x"] = {"treatment": "keep"}

    def extra_field(policy: dict) -> None:
        policy["note"] = "x"

    def id_with_two_columns(policy: dict) -> None:
        policy["pseudonyms"][0]["columns"] = ["ASSOCIATE ID - OWNER", "TITLE - OWNER"]

    def unknown_treatment(policy: dict) -> None:
        policy["treatments"]["TITLE - OWNER"] = {"treatment": "hash"}

    def blank_on_drop(policy: dict) -> None:
        policy["treatments"]["TITLE - OWNER"] = {"treatment": "drop", "blank_when": {"column": "TYPE - OWNER", "equals": "I"}}

    def other_condition(policy: dict) -> None:
        policy["pseudonyms"][0]["when"] = {"column": "TYPE - OWNER", "equals": "I"}
        policy["treatments"]["ASSOCIATE ID - OWNER"] = {"treatment": "keep", "blank_when": {"column": "TYPE - OWNER", "equals": "O"}}

    cases = {
        "policy_invalid_unlisted_column": (unlisted, "policy_invalid"),
        "pseudonym_source_not_dropped": (source_kept, "pseudonym_source_not_dropped"),
        "pseudonym_source_other_condition": (other_condition, "pseudonym_source_not_dropped"),
        "policy_invalid_duplicate_output": (duplicate_output, "policy_invalid"),
        "policy_invalid_output_is_header": (output_in_headers, "policy_invalid"),
        "policy_invalid_extra_field": (extra_field, "policy_invalid"),
        "policy_invalid_id_two_columns": (id_with_two_columns, "policy_invalid"),
        "policy_invalid_unknown_treatment": (unknown_treatment, "policy_invalid"),
        "policy_invalid_blank_on_drop": (blank_on_drop, "policy_invalid"),
        "unsupported_normalization": (lambda p: p.update(normalization_version="norm_v2"), "unsupported_normalization"),
        "policy_invalid_version": (lambda p: p.update(policy_version=2), "policy_invalid"),
        "policy_invalid_size": (lambda p: p.update(max_bytes=0), "policy_invalid"),
    }
    for name, (edit, code) in cases.items():
        folder = run.case_dir("policy_" + name)
        policy = changed(edit)
        expect_error(code, partial(module.transform, run.original, folder / "derivative", policy, "v1", reader_for(KEYS)))
        ensure(sorted(p.name for p in folder.iterdir()) == [], f"{name} left files behind")
    broken = run.case_dir("policy_unreadable") / "policy.json"
    broken.write_text('{"policy_id": ')
    expect_error("policy_unreadable", lambda: module.read_policy(broken))
    expect_error("policy_unreadable", lambda: module.read_policy(broken.parent / "absent.json"))
    ensure(module.read_policy(run.policy_path) == OWNER_POLICY, "the policy file did not read back")


def check_conditional_rows(run: Run) -> None:
    """Individual-owner rows keep their other cells; the name and city are replaced by a pseudonym; organisation rows keep every cell."""
    contract = module.transform(run.facilities, run.case_dir("facilities") / "derivative", FACILITY_POLICY, "v1", reader_for(KEYS))
    rows = table(run.root / "cases/facilities/derivative")
    pseudonym = "individual_owner_name_pseudonym"
    ensure(len(rows) == 3 == contract["output"]["row_count"], "a facility row was dropped")
    ensure(
        [(r["FAC_PAR_CORP_NAME"], r["FAC_PAR_CORP_CITY"]) for r in rows] == [("", ""), ("Acme Sample Corp", "Orgville"), ("", "")],
        "conditional blanking differs",
    )
    ensure(rows[0][pseudonym] == rows[2][pseudonym] != "" and rows[1][pseudonym] == "", "individual-owner pseudonyms differ")
    ensure([r["BEDS"] for r in rows] == ["50", "60", "70"] and contract["blanked_cells"] == 4, "kept cells or blank count differ")
    ensure(
        contract["conditionally_blanked_columns"] == ["FAC_PAR_CORP_NAME", "FAC_PAR_CORP_CITY"] and contract["dropped_columns"] == [], "contract columns differ"
    )
    leaked = "nova" in (run.root / "cases/facilities/derivative" / module.DATA_NAME).read_text().casefold()
    ensure(not leaked, "an individual owner name remained")


def check_admission(run: Run) -> None:
    """The derivative verifier accepts a made derivative and refuses each kind of tampering, without the original or the key."""
    folder = run.derivative("admit")
    ensure(module.verify_derivative(folder, OWNER_POLICY)["key_version"] == "v1", "a made derivative was refused")

    def tampered(name: str, edit: Callable[[Path], None], code: str, policy: dict | None = None) -> None:
        copy_path = run.case_dir("admit_" + name) / "derivative"
        shutil.copytree(folder, copy_path)
        edit(copy_path)
        expect_error(code, lambda: module.verify_derivative(copy_path, policy or OWNER_POLICY))

    def append_data(path: Path) -> None:
        with (path / module.DATA_NAME).open("a") as handle:
            handle.write("x\n")

    def edit_contract(path: Path) -> None:
        contract = json.loads((path / module.CONTRACT_NAME).read_text())
        contract["input"]["row_count"] = 99
        (path / module.CONTRACT_NAME).write_text(json.dumps(contract))

    def extra_file(path: Path) -> None:
        (path / "extra.txt").write_text("x")

    def raw_name(path: Path) -> None:
        reseal(path, lambda text: text.replace("v1:", "name:", 1))

    def wrong_version(path: Path) -> None:
        reseal(path, lambda text: text.replace("v1:", "v2:", 1))

    def drop_row(path: Path) -> None:
        reseal(path, lambda text: "\n".join(text.splitlines()[:-1]) + "\n")

    tampered("data_bytes", append_data, "derivative_hash_differs")
    tampered("contract_edit", edit_contract, "contract_hash_differs")
    tampered("extra_file", extra_file, "derivative_files_differ")
    tampered("pseudonym_format", raw_name, "pseudonym_format_differs")
    tampered("pseudonym_version", wrong_version, "pseudonym_format_differs")
    tampered("row_removed", drop_row, "row_count_differs")
    tampered("other_policy", lambda path: None, "policy_differs", {**OWNER_POLICY, "policy_id": "synthetic_other_policy"})
    expect_error("derivative_files_differ", lambda: module.verify_derivative(run.case_dir("admit_missing"), OWNER_POLICY))
    ensure(
        module.verify_derivative(folder, OWNER_POLICY)["contract_sha256"] == json.loads((folder / module.CONTRACT_NAME).read_text())["contract_sha256"],
        "verify changed",
    )


def check_command_line(run: Run) -> None:
    """The real command line: verify admits a derivative and refuses tampering; transform stops before any AWS call without settings; no value is printed."""
    folder = run.derivative("cli")
    code, output = run.cli("verify", "--policy", str(run.policy_path), "--output", str(folder))
    ensure(code == 0 and json.loads(output)["status"] == "admitted" and json.loads(output)["row_count"] == len(OWNER_ROWS), f"verify failed: {output}")
    tampered = run.case_dir("cli_tampered") / "derivative"
    shutil.copytree(folder, tampered)
    with (tampered / module.DATA_NAME).open("a") as handle:
        handle.write("x\n")
    code, output = run.cli("verify", "--policy", str(run.policy_path), "--output", str(tampered))
    ensure(
        code == 1 and json.loads(output.strip().splitlines()[-1]) == {"status": "rejected", "reason": "derivative_hash_differs"}, f"tampering passed: {output}"
    )
    target = run.case_dir("cli_transform")
    base = ["transform", "--policy", str(run.policy_path), "--input", str(run.original), "--output", str(target / "derivative"), "--key-version", "v1"]
    for name, extra in {
        "no_settings": [],
        "admin_profile": ["--profile", "owner_admin", "--region", "us-west-2"],
        "no_region": ["--profile", "project"],
    }.items():
        code, output = run.cli(*base, *extra)
        ensure(code == 1 and "aws_settings_invalid" in output, f"{name}: {output}")
        ensure(not (target / "derivative").exists(), f"{name}: output written")
    bad = run.case_dir("cli_bad_policy") / "policy.json"
    bad.write_text(json.dumps({**OWNER_POLICY, "policy_version": 2}))
    code, output = run.cli("transform", "--policy", str(bad), "--input", str(run.original), "--output", str(target / "derivative"), "--key-version", "v1")
    ensure(code == 1 and "policy_invalid" in output, f"bad policy passed: {output}")
    ensure(not [v for v in DIRECT_VALUES if v in output.casefold()], "a command line message holds a source value")


def check_no_secrets_or_leftovers(run: Run) -> None:
    """No key text, in either encoding, appears in any retained file; the original is unchanged."""
    needles = [text.encode() for text in (*KEYS.values(), CHANGED_V1)]
    needles += [base64.urlsafe_b64decode(text).hex().encode() for text in (*KEYS.values(), CHANGED_V1)]
    found = [str(p.relative_to(run.root)) for p in run.root.rglob("*") if p.is_file() and any(n in p.read_bytes() for n in needles)]
    ensure(not found, f"key text found in {found}")
    ensure(fingerprint(run.original) == run.original_print, "the original changed")
    strays = [str(p) for p in (run.root / "cases").rglob("pseudonymize_*")]
    ensure(not strays, f"staging folders remain: {strays}")


def main() -> None:
    """Run every case, write the artifact and exit nonzero when any case failed."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    run = Run(root)
    for check in (
        check_preservation,
        check_no_direct_identifiers,
        check_stability,
        check_equal_people,
        check_key_versions,
        check_idempotence,
        check_key_errors,
        check_input_errors,
        check_policy_errors,
        check_conditional_rows,
        check_admission,
        check_command_line,
    ):
        run.scenario(check.__name__.removeprefix("check_"), partial(check, run))
    run.scenario("no_secrets_or_leftovers", partial(check_no_secrets_or_leftovers, run))
    status = "passed" if all(c["passed"] for c in run.cases) else "failed"
    here = Path(__file__)
    artifact = {
        "kind": "synthetic_pseudonymization_e2e",
        "status": status,
        "passed": status == "passed",
        "completed_at_utc": datetime.now(UTC).isoformat(),
        "python_version": sys.version,
        "platform": sys.platform,
        "requirements_sha256": fingerprint(REPO_ROOT / "requirements.txt")[0],
        "implementation_sha256": fingerprint(here.with_name("pseudonymize.py"))[0],
        "runner_sha256": fingerprint(here)[0],
        "cases": run.cases,
        "files": [{"path": p, "sha256": digest[0]} for p, digest in tree(root).items()],
        "fixture_scope": "Invented names and identifiers only; keys built from labels, never stored",
        "network_calls": 0,
        "aws_calls": 0,
        "reproduce": [sys.executable, "-m", "scripts.acquisition.run_pseudonymize_e2e", "--output", "<new_directory>"],
        "prerequisites": ["Run from the repository root with the project .venv and the pinned requirements.txt dependencies."],
        "reset": "Use a new output directory; existing evidence is preserved.",
        "tested_boundary": "Synthetic CSV files through the transformation, its Parameter Store reader with a stand-in client and the command line.",
        "untested_boundaries": [
            "Real Parameter Store and its IAM grant",
            "Real originals, real column layouts and their free text",
            "Downstream schema use of the pseudonyms",
            "Key rotation and recovery procedures",
        ],
        "cleanup": "Synthetic fixtures and results retained in this folder; no process, cloud resource or key file remains.",
    }
    write_once(root / "artifact.json", encoded_json(artifact))
    sys.stdout.write(
        json.dumps({"status": status, "cases": len(run.cases), "artifact": str(root / "artifact.json"), "sha256": canonical_hash(artifact)}) + "\n"
    )
    if status != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
