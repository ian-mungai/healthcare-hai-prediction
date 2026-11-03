"""Repeatable synthetic verification of the keyed pseudonymization and its admission contract: no real data, no AWS call, no real key.

Every input is generated here from invented names. The key reaches the transformation only through its key-reader seam, as text built from a label.
"""

from __future__ import annotations

import argparse
import base64
import copy
import csv
import hashlib
import hmac
import json
import logging
import os
import shutil
import socket
import sys
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import boto3
from botocore.config import Config
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
INDIVIDUAL = {"column": "LICEE_TOC", "equals": "Investor - Individual", "others": ["Investor - Corporation"]}
FACILITY_HEADERS = ["FAC_NO", "LICEE_TOC", "FAC_PAR_CORP_NAME", "FAC_PAR_CORP_CITY", "BEDS"]
FACILITY_ROWS = [
    ["100", "Investor - Individual", "Nova Sample", "Sampletown", "50"],
    ["101", "Investor - Corporation", "Acme Sample Corp", "Orgville", "60"],
    ["102", "Investor - Individual", "NOVA  sample", "Sampletown", "70"],
    ["103", "Investor - Individual ", "Nova Sample", "Sampletown", "80"],
    ["104", "INVESTOR\u00a0-  individual", "Pat Example", "Sampletown", "90"],
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
SCRUB_HEADERS = ["ROW", "ASSOC ID", "NAME", "NOTE"]
SCRUB_ROWS = [
    ["1", "0123456789", "Zelda", "Plain Co"],
    ["2", "", "", "Zelda Holdings LLC"],
    ["3", "", "", "Owner 0123456789 LLC"],
    ["4", "", "", "Owner 01234 56789 LLC"],
    ["5", "", "", "Unrelated Clinic"],
    ["6", "", "", "Ref 0123456789-A"],
    ["7", "", "", "Zelda's Clinic"],
    ["8", "", "", "Zelda\u2019s Clinic"],
    ["9", "", "", "O'Hare Plain Co"],
    ["10", "", "Ali", "ALİ's Clinic"],
    ["11", "", "O'S", "O'S Clinic"],
    ["12", "", "", "ALİ\u2019s Clinic"],
    ["13", "", "", "Zelda's_Clinic"],
]
SCRUB_POLICY: dict[str, Any] = {
    "policy_id": "synthetic_scrub_rows",
    "policy_version": 1,
    "source_id": "SYNTHETIC_SCRUB",
    "normalization_version": "norm_v1",
    "headers": SCRUB_HEADERS,
    "treatments": {
        "ROW": {"treatment": "keep"},
        "ASSOC ID": {"treatment": "drop"},
        "NAME": {"treatment": "drop"},
        "NOTE": {"treatment": "scrub_names"},
    },
    "pseudonyms": [
        {"output": "person_id_pseudonym", "kind": "id", "namespace": "synthetic_associate_id", "columns": ["ASSOC ID"]},
        {"output": "person_name_match_pseudonym", "kind": "name", "namespace": "person_name", "columns": ["NAME"]},
    ],
    "max_bytes": 1024**2,
    "min_rows": 1,
}
KNOWN_ANSWER_ID = "v1:4f9e8fdc1d977ef991d37592f3ffa7f0caafc2cc2b460908b79665e4195bc3de"
KNOWN_ANSWER_NAME = "v1:1297f512fd485a72e26986d6f01d72f6443021b8f03e0fb53705da9161cab126"
NO_NETWORK_MESSAGE = "network access attempted"
LOOPBACK = "127.0.0.1"
REAL_CONNECT = socket.socket.connect


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


def reseal(directory: Path, edit_data: Callable[[str], str] | None = None, edit_contract: Callable[[dict], None] | None = None) -> str:
    """Edit a copied derivative, bind its contract to the edited bytes and return the new contract digest, to reach checks beyond the hash check."""
    data = directory / module.DATA_NAME
    if edit_data is not None:
        data.write_text(edit_data(data.read_text()))
    contract = json.loads((directory / module.CONTRACT_NAME).read_text())
    contract["output"]["sha256"], contract["output"]["byte_count"] = fingerprint(data)
    if edit_contract is not None:
        edit_contract(contract)
    contract.pop("contract_sha256", None)
    contract["contract_sha256"] = canonical_hash(contract)
    (directory / module.CONTRACT_NAME).write_text(json.dumps(contract))
    return str(contract["contract_sha256"])


def refuse_network(sock: socket.socket, address: Any, *rest: Any) -> Any:
    """Replace socket connection so a case that reaches any address but the loopback stand-in fails."""
    if isinstance(address, tuple) and address[0] == LOOPBACK:
        return REAL_CONNECT(sock, address, *rest)
    raise AssertionError(NO_NETWORK_MESSAGE)


STAND_IN_CREDENTIAL = "stand-in"


class StandInParameters:
    """A Parameter Store client stand-in with the one call the key reader makes."""

    def __init__(
        self, values: dict[str, str], failure: Exception | None = None, parameter_type: str = "SecureString", returned_name: str | None = None
    ) -> None:
        self.values, self.failure, self.parameter_type, self.returned_name = values, failure, parameter_type, returned_name
        self.calls: list[tuple[str, bool]] = []

    def get_parameter(self, Name: str, WithDecryption: bool) -> dict:
        """Record the request, then fail, miss or answer as configured."""
        self.calls.append((Name, WithDecryption))
        if self.failure is not None:
            raise self.failure
        if Name not in self.values:
            raise ClientError({"Error": {"Code": "ParameterNotFound", "Message": "missing"}}, "GetParameter")
        return {"Parameter": {"Name": self.returned_name or Name, "Type": self.parameter_type, "Value": self.values[Name]}}


class SsmHandler(BaseHTTPRequestHandler):
    """Answers the Parameter Store JSON protocol for GetParameter on the loopback address."""

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:
        """Record the request and answer with the configured value, or ParameterNotFound."""
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))))
        stand_in = cast(SsmStandIn, self.server)
        stand_in.requests.append((self.headers.get("X-Amz-Target", ""), body))
        value = stand_in.values.get(body.get("Name"))
        status, payload = (
            (400, {"__type": "ParameterNotFound", "message": "missing"})
            if value is None
            else (200, {"Parameter": {"Name": body["Name"], "Type": "SecureString", "Value": value, "Version": 1}})
        )
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/x-amz-json-1.1")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class SsmStandIn(ThreadingHTTPServer):
    """A loopback Parameter Store server that records each request; no AWS service is contacted."""

    def __init__(self, values: dict[str, str]) -> None:
        super().__init__((LOOPBACK, 0), SsmHandler)
        self.values = values
        self.requests: list[tuple[str, dict]] = []


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
        except (AssertionError, PseudonymizationError, OSError, KeyError, ValueError, TypeError) as error:
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
    late = run.case_dir("interrupted_after_rename")
    real_rename = os.rename

    def rename_then_fail(source: str, target: str) -> None:
        real_rename(source, target)
        raise OSError("stopped after the rename")

    with patch.object(module.os, "rename", rename_then_fail):
        try:
            module.transform(run.original, late / "derivative", OWNER_POLICY, "v1", reader_for(KEYS))
        except OSError:
            pass
        else:
            raise AssertionError("the run stopped after the rename did not fail")
    ensure(sorted(p.name for p in late.iterdir()) == ["derivative"], "a run stopped after the rename left staging files")
    ensure(
        module.verify_derivative(late / "derivative", OWNER_POLICY, first["contract_sha256"])["contract_sha256"] == first["contract_sha256"],
        "the result is incomplete",
    )
    after = tree(late / "derivative")
    ensure(module.transform(run.original, late / "derivative", OWNER_POLICY, "v1", reader_for(KEYS)) == first, "the rerun after the rename differs")
    ensure(tree(late / "derivative") == after and sorted(p.name for p in late.iterdir()) == ["derivative"], "the rerun after the rename changed state")
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
        policy["pseudonyms"][0]["when"] = {"column": "TYPE - OWNER", "equals": "I", "others": ["O"]}
        policy["treatments"]["ASSOCIATE ID - OWNER"] = {"treatment": "keep", "blank_when": {"column": "TYPE - OWNER", "equals": "O", "others": ["I"]}}

    def kind_list(policy: dict) -> None:
        policy["pseudonyms"][0]["kind"] = []

    def kind_dict(policy: dict) -> None:
        policy["pseudonyms"][1]["kind"] = {}

    def namespace_list(policy: dict) -> None:
        policy["pseudonyms"][0]["namespace"] = ["x"]

    def columns_nested(policy: dict) -> None:
        policy["pseudonyms"][1]["columns"] = [["FIRST NAME - OWNER"], "LAST NAME - OWNER"]

    def treatment_list(policy: dict) -> None:
        policy["treatments"]["TITLE - OWNER"] = {"treatment": []}

    def middle_list(policy: dict) -> None:
        policy["pseudonyms"][1]["middle_column"] = []

    cases = {
        "policy_invalid_version_true": (lambda p: p.update(policy_version=True), "policy_invalid"),
        "policy_invalid_version_float": (lambda p: p.update(policy_version=1.0), "policy_invalid"),
        "policy_invalid_min_rows_true": (lambda p: p.update(min_rows=True), "policy_invalid"),
        "policy_invalid_kind_list": (kind_list, "policy_invalid"),
        "policy_invalid_kind_dict": (kind_dict, "policy_invalid"),
        "policy_invalid_namespace_list": (namespace_list, "policy_invalid"),
        "policy_invalid_columns_nested": (columns_nested, "policy_invalid"),
        "policy_invalid_treatment_list": (treatment_list, "policy_invalid"),
        "policy_invalid_middle_list": (middle_list, "policy_invalid"),
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
    ensure(len(rows) == 5 == contract["output"]["row_count"], "a facility row was dropped")
    blanked = [(r["FAC_PAR_CORP_NAME"], r["FAC_PAR_CORP_CITY"]) for r in rows]
    ensure(blanked == [("", ""), ("Acme Sample Corp", "Orgville"), ("", ""), ("", ""), ("", "")], f"conditional blanking differs: {blanked}")
    ensure(rows[0][pseudonym] == rows[2][pseudonym] == rows[3][pseudonym] != "" and rows[1][pseudonym] == "", "individual-owner pseudonyms differ")
    ensure(rows[4][pseudonym] not in ("", rows[0][pseudonym]), "a second individual owner shares or lacks a pseudonym")
    ensure([r["BEDS"] for r in rows] == ["50", "60", "70", "80", "90"] and contract["blanked_cells"] == 8, "kept cells or blank count differ")
    ensure(
        contract["conditionally_blanked_columns"] == ["FAC_PAR_CORP_NAME", "FAC_PAR_CORP_CITY"] and contract["dropped_columns"] == [], "contract columns differ"
    )
    text = (run.root / "cases/facilities/derivative" / module.DATA_NAME).read_text().casefold()
    ensure("nova" not in text and "pat example" not in text and "sampletown" not in text, "an individual owner value remained")


def check_discriminator(run: Run) -> None:
    """A discriminator value outside the approved set stops the run before any output; approved variants are normalized; the condition must be well formed."""
    for name, value in {"partnership": "Investor - Partnership", "empty": "", "extra_word": "Investor - Individual Trust"}.items():
        folder = run.case_dir("discriminator_" + name)
        source = write_csv(folder / "in.csv", FACILITY_HEADERS, [["1", value, "Nova Sample", "Sampletown", "5"]])
        expect_error("discriminator_unknown", partial(module.transform, source, folder / "derivative", FACILITY_POLICY, "v1", reader_for(KEYS)))
        ensure(sorted(p.name for p in folder.iterdir()) == ["in.csv"], f"{name} left files behind")

    def with_condition(edit: Callable[[dict], None]) -> dict:
        policy = copy.deepcopy(FACILITY_POLICY)
        for rule in (policy["treatments"]["FAC_PAR_CORP_NAME"], policy["treatments"]["FAC_PAR_CORP_CITY"], policy["pseudonyms"][0]):
            edit(rule.get("blank_when") or rule["when"])
        return policy

    broken = {
        "others_missing": lambda c: c.pop("others", None),
        "others_not_list": lambda c: c.update(others="Investor - Corporation"),
        "others_nested": lambda c: c.update(others=[["x"]]),
        "equals_among_others": lambda c: c.update(others=["INVESTOR - individual "]),
        "others_duplicate": lambda c: c.update(others=["a", "A "]),
    }
    for name, edit in broken.items():
        folder = run.case_dir("condition_" + name)
        expect_error("policy_invalid", partial(module.transform, run.facilities, folder / "derivative", with_condition(edit), "v1", reader_for(KEYS)))
        ensure(sorted(p.name for p in folder.iterdir()) == [], f"{name} left files behind")


def check_scrub(run: Run) -> None:
    """The scrub covers known names, also possessive, and known identifiers, also typed with spaces or a suffix; short values fail closed."""
    folder = run.case_dir("scrub")
    source = write_csv(folder / "in.csv", SCRUB_HEADERS, SCRUB_ROWS)
    contract = module.transform(source, folder / "derivative", SCRUB_POLICY, "v1", reader_for(KEYS))
    rows = table(folder / "derivative")
    notes = [r["NOTE"] for r in rows]
    ensure(
        notes
        == [
            "Plain Co",
            module.REPLACEMENT,
            module.REPLACEMENT,
            module.REPLACEMENT,
            "Unrelated Clinic",
            module.REPLACEMENT,
            module.REPLACEMENT,
            module.REPLACEMENT,
            "O'Hare Plain Co",
            *[module.REPLACEMENT] * 4,
        ],
        f"scrub differs: {notes}",
    )
    ensure(contract["scrubbed_cells"] == 10, f"scrubbed count differs: {contract['scrubbed_cells']}")
    text = "".join((folder / "derivative" / n).read_text().casefold() for n in (module.DATA_NAME, module.CONTRACT_NAME))
    ensure("zelda" not in text and "0123456789" not in text and "01234 56789" not in text, "a known name or identifier remained")
    for name, edit, code in (
        ("short_identifier", lambda r: r.__setitem__(1, "12"), "identifier_too_short"),
        ("short_name", lambda r: r.__setitem__(2, "A"), "name_too_short"),
    ):
        case = run.case_dir("scrub_" + name)
        rows_in = copy.deepcopy(SCRUB_ROWS)
        edit(rows_in[0])
        bad = write_csv(case / "in.csv", SCRUB_HEADERS, rows_in)
        expect_error(code, partial(module.transform, bad, case / "derivative", SCRUB_POLICY, "v1", reader_for(KEYS)))
        ensure(sorted(p.name for p in case.iterdir()) == ["in.csv"], f"{name} left files behind")
        plain = copy.deepcopy(SCRUB_POLICY)
        plain["treatments"]["NOTE"] = {"treatment": "keep"}
        module.transform(bad, case / "unscrubbed", plain, "v1", reader_for(KEYS))


MIXED_ROWS = [
    ["1", "", "O'S Zelda", "Plain Co"],
    ["2", "", "", "O'S Zelda's Clinic"],
    ["3", "", "", "O\u2019S Zelda\uff07s Clinic"],
    ["4", "", "", "O'S Zelda Clinic"],
    ["5", "", "", "Zelda's Clinic"],
]


def check_scrub_mixed_possessive(run: Run) -> None:
    """A multi-word known name is found when only some of its words carry a possessive ending, in any apostrophe form."""
    folder = run.case_dir("scrub_mixed_possessive")
    source = write_csv(folder / "in.csv", SCRUB_HEADERS, MIXED_ROWS)
    contract = module.transform(source, folder / "derivative", SCRUB_POLICY, "v1", reader_for(KEYS))
    notes = [r["NOTE"] for r in table(folder / "derivative")]
    ensure(notes == ["Plain Co", module.REPLACEMENT, module.REPLACEMENT, module.REPLACEMENT, "Zelda's Clinic"], f"mixed possessive scrub differs: {notes}")
    ensure(contract["scrubbed_cells"] == 3, f"mixed possessive count differs: {contract['scrubbed_cells']}")


TEXT_WORD_ROWS = [
    ["1", "", "Zelda Na", "Plain Co"],
    ["2", "", "Yara Quimby", "Plain Co"],
    ["3", "", "Wren D''Arcy", "Plain Co"],
    ["4", "", "J.R. Vale", "Plain Co"],
    ["5", "", "O'S", "Plain Co"],
    ["6", "", "", "Zelda Na Clinic"],
    ["7", "", "", "Yara Unknown Quimby Clinic"],
    ["8", "", "", "Wren D''Arcy Clinic"],
    ["9", "", "", "J..R. Vale Clinic"],
    ["10", "", "", "O'S's Clinic"],
]


def check_scrub_text_words(run: Run) -> None:
    """Text words keep words that are empty markers in identity fields, accept repeated connectors and strip one possessive, without widening."""
    folder = run.case_dir("scrub_text_words")
    source = write_csv(folder / "in.csv", SCRUB_HEADERS, TEXT_WORD_ROWS)
    contract = module.transform(source, folder / "derivative", SCRUB_POLICY, "v1", reader_for(KEYS))
    notes = [r["NOTE"] for r in table(folder / "derivative")]
    expected = ["Plain Co"] * 5 + [module.REPLACEMENT, "Yara Unknown Quimby Clinic", module.REPLACEMENT, module.REPLACEMENT, module.REPLACEMENT]
    ensure(notes == expected, f"text word scrub differs: {notes}")
    ensure(contract["scrubbed_cells"] == 4, f"text word count differs: {contract['scrubbed_cells']}")


APOSTROPHE_FORMS = ("'", "\u2018", "\u2019", "\u02bc", "`", "\u00b4", "\uff07")


def check_scrub_apostrophe_forms(run: Run) -> None:
    """Every accepted apostrophe form is recognised as a possessive on the first word and on the last word of a two-word known name."""
    rows = [["1", "", "Zelda Quimby", "Plain Co"]]
    for form in APOSTROPHE_FORMS:
        rows.append([str(len(rows) + 1), "", "", f"Zelda{form}s Quimby Clinic"])
        rows.append([str(len(rows) + 1), "", "", f"Zelda Quimby{form}s Clinic"])
    folder = run.case_dir("scrub_apostrophe_forms")
    source = write_csv(folder / "in.csv", SCRUB_HEADERS, rows)
    contract = module.transform(source, folder / "derivative", SCRUB_POLICY, "v1", reader_for(KEYS))
    notes = [r["NOTE"] for r in table(folder / "derivative")]
    survived = [n for n in notes[1:] if n != module.REPLACEMENT]
    ensure(not survived, f"apostrophe forms survived: {survived}")
    ensure(contract["scrubbed_cells"] == 2 * len(APOSTROPHE_FORMS), f"apostrophe form count differs: {contract['scrubbed_cells']}")


SOURCE_SPELLING_ROWS = [
    ["1", "", "O\u00b4Brien", "Plain Co"],
    ["2", "", "Zelda D\u00b4Arcy", "Plain Co"],
    ["3", "", "", "O\u00b4Brien Clinic"],
    ["4", "", "", "O\u00b4Brien's Clinic"],
    ["5", "", "", "O'Brien Clinic"],
    ["6", "", "", "Zelda D\u00b4Arcy Clinic"],
    ["7", "", "", "Zelda D\u2019Arcy Clinic"],
    ["8", "", "", "Brien Clinic"],
]


SPLIT_HEADERS = ["ROW", "FIRST", "MIDDLE", "LAST", "NOTE"]
SPLIT_POLICY: dict[str, Any] = {
    "policy_id": "synthetic_split_name_rows",
    "policy_version": 1,
    "source_id": "SYNTHETIC_SPLIT",
    "normalization_version": "norm_v1",
    "headers": SPLIT_HEADERS,
    "treatments": {
        "ROW": {"treatment": "keep"},
        "FIRST": {"treatment": "drop"},
        "MIDDLE": {"treatment": "drop"},
        "LAST": {"treatment": "drop"},
        "NOTE": {"treatment": "scrub_names"},
    },
    "pseudonyms": [
        {"output": "person_name_match_pseudonym", "kind": "name", "namespace": "person_name", "columns": ["FIRST", "LAST"], "middle_column": "MIDDLE"},
    ],
    "max_bytes": 1024**2,
    "min_rows": 1,
}
SPLIT_ROWS = [
    ["1", "Zelda", "", "Na", "Plain Co"],
    ["2", "Yara", "Na", "Quimby", "Plain Co"],
    ["3", "", "", "", "Zelda Na Clinic"],
    ["4", "", "", "", "Yara Na Quimby Clinic"],
    ["5", "", "", "", "Yara Quimby Clinic"],
    ["6", "", "", "", "Unrelated Na Clinic"],
]


def check_scrub_split_empty_markers(run: Run) -> None:
    """Known names whose words sit in separate first, middle and last columns keep their empty-marker words for scrubbing (HMAC rules unchanged)."""
    folder = run.case_dir("scrub_split_empty_markers")
    source = write_csv(folder / "in.csv", SPLIT_HEADERS, SPLIT_ROWS)
    contract = module.transform(source, folder / "derivative", SPLIT_POLICY, "v1", reader_for(KEYS))
    notes = [r["NOTE"] for r in table(folder / "derivative")]
    expected = ["Plain Co", "Plain Co", module.REPLACEMENT, module.REPLACEMENT, module.REPLACEMENT, "Unrelated Na Clinic"]
    ensure(notes == expected, f"split-column empty-marker scrub differs: {notes}")
    ensure(contract["scrubbed_cells"] == 3, f"split-column count differs: {contract['scrubbed_cells']}")


def check_scrub_source_spellings(run: Run) -> None:
    """Known names whose source spelling holds an apostrophe form are matched with the same word rules as the text, in every apostrophe form."""
    folder = run.case_dir("scrub_source_spellings")
    source = write_csv(folder / "in.csv", SCRUB_HEADERS, SOURCE_SPELLING_ROWS)
    contract = module.transform(source, folder / "derivative", SCRUB_POLICY, "v1", reader_for(KEYS))
    notes = [r["NOTE"] for r in table(folder / "derivative")]
    expected = ["Plain Co", "Plain Co", *[module.REPLACEMENT] * 5, "Brien Clinic"]
    ensure(notes == expected, f"source spelling scrub differs: {notes}")
    ensure(contract["scrubbed_cells"] == 5, f"source spelling count differs: {contract['scrubbed_cells']}")


def check_known_answer(run: Run) -> None:
    """The pseudonym equals an independently computed HMAC and the fixed known answer of the independent tool."""
    rows = table(run.derivative("known_answer"))
    ensure(rows[0]["owner_person_pseudonym"] == KNOWN_ANSWER_ID, "identifier pseudonym differs from the known answer")
    ensure(rows[0]["owner_name_match_pseudonym"] == KNOWN_ANSWER_NAME, "name pseudonym differs from the known answer")
    key = hashlib.sha256(b"synthetic key one").digest()
    fields = [b"hai-pseudonym", b"norm_v1", b"synthetic_associate_id", b"id", b"1", b"0123456789"]
    ensure("v1:" + hmac.new(key, b"\x1f".join(fields), hashlib.sha256).hexdigest() == rows[0]["owner_person_pseudonym"], "independent HMAC differs")
    fields = [b"hai-pseudonym", b"norm_v1", b"person_name", b"name", b"2", b"zelda", b"quimby"]
    ensure("v1:" + hmac.new(key, b"\x1f".join(fields), hashlib.sha256).hexdigest() == rows[0]["owner_name_match_pseudonym"], "independent name HMAC differs")


def check_isolation(run: Run) -> None:
    """The namespace, the kind and the part boundaries each change a pseudonym on their own."""

    def pseudonyms(name: str, headers: list[str], data: list[list[str]], rules: list[dict]) -> list[dict[str, str]]:
        policy = {
            **copy.deepcopy(SCRUB_POLICY),
            "policy_id": name,
            "headers": headers,
            "treatments": {h: {"treatment": "drop"} for h in headers},
            "pseudonyms": rules,
        }
        folder = run.case_dir("isolation_" + name)
        module.transform(write_csv(folder / "in.csv", headers, data), folder / "derivative", policy, "v1", reader_for(KEYS))
        return table(folder / "derivative")

    def rule(output: str, kind: str, namespace: str, columns: list[str]) -> dict:
        return {"output": output, "kind": kind, "namespace": namespace, "columns": columns}

    one = pseudonyms(
        "one",
        ["V"],
        [["zelda"]],
        [rule("a_pseudonym", "id", "ns_one", ["V"]), rule("b_pseudonym", "name", "ns_one", ["V"]), rule("c_pseudonym", "id", "ns_two", ["V"])],
    )[0]
    ensure(len({one["a_pseudonym"], one["b_pseudonym"], one["c_pseudonym"]}) == 3, "the kind or the namespace did not change the pseudonym")
    parts = pseudonyms("parts", ["F", "L"], [["ab", "c"], ["a", "bc"], ["a b", "c"], ["a", "b c"]], [rule("n_pseudonym", "name", "person_name", ["F", "L"])])
    ensure(len({r["n_pseudonym"] for r in parts}) == 4, "a part boundary did not change the pseudonym")
    joined = pseudonyms("joined", ["F"], [["ab c"]], [rule("n_pseudonym", "name", "person_name", ["F"])])[0]["n_pseudonym"]
    ensure(joined not in {r["n_pseudonym"] for r in parts}, "the part count did not change the pseudonym")


def check_sdk_logging(run: Run) -> None:
    """Through the real SDK on loopback, DEBUG logs hold no decrypted value, the request asks for decryption and the log state is restored."""
    server = SsmStandIn({module.PARAMETER_PREFIX + "v1": KEYS["v1"]})
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    records: list[str] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record.getMessage())

    handler, root_logger, sdk_logger = Capture(), logging.getLogger(), logging.getLogger("botocore")
    saved = (root_logger.level, sdk_logger.level, logging.root.manager.disable)
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.DEBUG)
    sdk_logger.setLevel(logging.DEBUG)
    try:
        session = boto3.Session(aws_access_key_id=STAND_IN_CREDENTIAL, aws_secret_access_key=STAND_IN_CREDENTIAL, region_name="us-west-2")
        client = session.client("ssm", endpoint_url=f"http://{LOOPBACK}:{server.server_address[1]}", config=Config(**module.CLIENT_OPTIONS))
        client.get_parameter(Name=module.PARAMETER_PREFIX + "v1", WithDecryption=True)
        ensure(any(KEYS["v1"] in line for line in records), "control: the SDK did not log the response body, so this case proves nothing")
        records.clear()
        folder = run.case_dir("sdk_logging")
        contract = module.transform(run.original, folder / "derivative", OWNER_POLICY, "v1", module.parameter_store_reader(client))
        ensure(not [line for line in records if KEYS["v1"] in line], "the SDK logged the decrypted key during the read")
        ensure(contract == run.make("sdk_logging_reference"), "the SDK read gave another result")
        ensure(logging.root.manager.disable == saved[2], "the read left the logging switch changed")
        ensure(
            server.requests[-1][0] == "AmazonSSM.GetParameter" and server.requests[-1][1]["WithDecryption"] is True, "the request did not ask for decryption"
        )
        expect_error("key_missing", partial(module.transform, run.original, folder / "other", OWNER_POLICY, "v2", module.parameter_store_reader(client)))
        ensure(not [line for line in records if KEYS["v1"] in line], "a log line holds the key after a missing read")
    finally:
        root_logger.removeHandler(handler)
        root_logger.setLevel(saved[0])
        sdk_logger.setLevel(saved[1])
        server.shutdown()
        server.server_close()
        thread.join(timeout=10)
    ensure(not thread.is_alive(), "the stand-in server thread did not stop")


def check_parameter_checks(run: Run) -> None:
    """The key parameter must be a SecureString and must come back under the requested name."""
    name = module.PARAMETER_PREFIX + "v1"
    for label, stand_in in {
        "plain_string": StandInParameters({name: KEYS["v1"]}, parameter_type="String"),
        "string_list": StandInParameters({name: KEYS["v1"]}, parameter_type="StringList"),
        "other_name": StandInParameters({name: KEYS["v1"]}, returned_name=module.PARAMETER_PREFIX + "v2"),
    }.items():
        folder = run.case_dir("parameter_" + label)
        expect_error(
            "key_parameter_invalid", partial(module.transform, run.original, folder / "d", OWNER_POLICY, "v1", module.parameter_store_reader(stand_in))
        )
        ensure(sorted(p.name for p in folder.iterdir()) == [], f"{label} left files behind")
    good = StandInParameters({name: KEYS["v1"]})
    ensure(module.parameter_store_reader(good)("v1") == KEYS["v1"], "a SecureString under the requested name was refused")


def digest_of(directory: Path) -> str:
    """The contract digest of a made derivative, as the run prints it for the owner to keep."""
    return str(json.loads((directory / module.CONTRACT_NAME).read_text())["contract_sha256"])


def check_admission(run: Run) -> None:
    """The verifier admits a made derivative against its trusted digest and refuses each tampering, even resealed, without the original or the key."""
    folder = run.derivative("admit")
    trusted = digest_of(folder)
    ensure(module.verify_derivative(folder, OWNER_POLICY, trusted)["key_version"] == "v1", "a made derivative was refused")

    def tampered(name: str, edit: Callable[[Path], str | None], code: str, policy: dict | None = None, own_digest: bool = False) -> None:
        copy_path = run.case_dir("admit_" + name) / "derivative"
        shutil.copytree(folder, copy_path)
        resealed = edit(copy_path)
        digest = resealed if own_digest and resealed else trusted
        expect_error(code, partial(module.verify_derivative, copy_path, policy or OWNER_POLICY, digest))

    def append_data(path: Path) -> None:
        with (path / module.DATA_NAME).open("a") as handle:
            handle.write("x\n")

    def edit_contract(path: Path) -> None:
        contract = json.loads((path / module.CONTRACT_NAME).read_text())
        contract["input"]["row_count"] = 99
        (path / module.CONTRACT_NAME).write_text(json.dumps(contract))

    def extra_file(path: Path) -> None:
        (path / "extra.txt").write_text("x")

    def lines(text: str) -> list[str]:
        return text.splitlines()

    def first_value(text: str) -> str:
        return text[text.index("v1:") : text.index("v1:") + 67]

    def swapped(text: str) -> str:
        rows = lines(text)
        rows[1], rows[2] = rows[2], rows[1]
        return "\n".join(rows) + "\n"

    def widened(text: str) -> str:
        rows = lines(text)
        rows[1] += ",x"
        return "\n".join(rows) + "\n"

    def counted(contract: dict) -> None:
        contract["pseudonyms"][0]["written"] += 1

    def other_hold(contract: dict) -> None:
        contract["model_eligible"] = True

    def raw_key_check(contract: dict) -> None:
        contract["key_check"] = "zelda quimby"

    def extra_field(contract: dict) -> None:
        contract["note"] = "extra"

    tampered("data_bytes", lambda path: append_data(path), "derivative_hash_differs")
    tampered("contract_edit", lambda path: edit_contract(path), "contract_hash_differs")
    tampered("extra_file", lambda path: extra_file(path), "derivative_files_differ")
    tampered("pseudonym_format", lambda path: reseal(path, lambda t: t.replace("v1:", "name:", 1)), "pseudonym_format_differs", own_digest=True)
    tampered("pseudonym_version", lambda path: reseal(path, lambda t: t.replace("v1:", "v2:", 1)), "pseudonym_format_differs", own_digest=True)
    tampered("row_removed", lambda path: reseal(path, lambda t: "\n".join(lines(t)[:-1]) + "\n"), "row_count_differs", own_digest=True)
    tampered("other_policy", lambda path: None, "policy_differs", {**OWNER_POLICY, "policy_id": "synthetic_other_policy"})
    tampered(
        "added_column",
        lambda path: reseal(path, lambda t: "\n".join(f"{row},NAME" if i == 0 else f"{row},x" for i, row in enumerate(lines(t))) + "\n"),
        "derivative_schema_differs",
        own_digest=True,
    )
    tampered("wider_row", lambda path: reseal(path, widened), "row_width_differs", own_digest=True)
    tampered("changed_hold", lambda path: reseal(path, edit_contract=other_hold), "contract_schema_differs", own_digest=True)
    tampered("raw_key_check", lambda path: reseal(path, edit_contract=raw_key_check), "contract_schema_differs", own_digest=True)
    tampered("extra_contract_field", lambda path: reseal(path, edit_contract=extra_field), "contract_schema_differs", own_digest=True)
    tampered("changed_count", lambda path: reseal(path, edit_contract=counted), "contract_counts_differ", own_digest=True)
    tampered("reordered_rows", lambda path: reseal(path, swapped), "trusted_digest_differs")
    tampered("replaced_pseudonym", lambda path: reseal(path, lambda t: t.replace(first_value(t), "v1:" + "0" * 64, 1)), "trusted_digest_differs")
    reordered = run.case_dir("admit_reordered_limit") / "derivative"
    shutil.copytree(folder, reordered)
    own = reseal(reordered, swapped)
    ensure(
        module.verify_derivative(reordered, OWNER_POLICY, own)["contract_sha256"] == own, "limit: resealed reordering is found only through the trusted digest"
    )
    expect_error("trusted_digest_differs", partial(module.verify_derivative, folder, OWNER_POLICY, "0" * 64))
    for bad in ("ABC", "", "A" * 64, None):
        expect_error("invalid_hash_pin", partial(module.verify_derivative, folder, OWNER_POLICY, cast(Any, bad)))
    expect_error("derivative_files_differ", partial(module.verify_derivative, run.case_dir("admit_missing"), OWNER_POLICY, trusted))

    facilities = run.case_dir("admit_facilities")
    module.transform(run.facilities, facilities / "derivative", FACILITY_POLICY, "v1", reader_for(KEYS))
    facility_digest = digest_of(facilities / "derivative")
    ensure(
        module.verify_derivative(facilities / "derivative", FACILITY_POLICY, facility_digest)["key_version"] == "v1", "a made facility derivative was refused"
    )

    def more_pseudonyms(contract: dict) -> None:
        contract["pseudonyms"][0].update(written=5, withheld=0, distinct=3)

    for name, edit, adjust in (
        ("pseudonym_on_organisation", lambda t: t.replace("Acme Sample Corp,Orgville,60,", "Acme Sample Corp,Orgville,60,v1:" + "1" * 64, 1), more_pseudonyms),
        ("city_kept_on_individual", lambda t: t.replace("Individual,,,50", "Individual,,Sampletown,50", 1), None),
    ):
        copy_path = run.case_dir("admit_facility_" + name) / "derivative"
        shutil.copytree(facilities / "derivative", copy_path)
        resealed = reseal(copy_path, edit, adjust)
        expect_error("conditional_rows_differ", partial(module.verify_derivative, copy_path, FACILITY_POLICY, resealed))


def check_command_line(run: Run) -> None:
    """The real command line: verify needs a trusted digest and refuses resealing; transform stops before any AWS call; no value is printed."""
    folder = run.derivative("cli")
    trusted = digest_of(folder)
    arguments = ["verify", "--policy", str(run.policy_path), "--expect-contract-sha256", trusted, "--output"]
    code, output = run.cli(*arguments, str(folder))
    ensure(code == 0 and json.loads(output)["status"] == "admitted" and json.loads(output)["row_count"] == len(OWNER_ROWS), f"verify failed: {output}")
    code, output = run.cli("verify", "--policy", str(run.policy_path), "--output", str(folder))
    ensure(code == 2 and "--expect-contract-sha256" in output, f"verify ran without a trusted digest: {output}")
    tampered = run.case_dir("cli_tampered") / "derivative"
    shutil.copytree(folder, tampered)
    ensure(reseal(tampered) == trusted, "an unchanged reseal changed the digest")
    code, output = run.cli(*arguments, str(tampered))
    ensure(code == 0, f"an unchanged resealed derivative was refused: {output}")
    reseal(tampered, lambda text: "\n".join([*text.splitlines()[:1], *reversed(text.splitlines()[1:])]) + "\n")
    code, output = run.cli(*arguments, str(tampered))
    ensure(
        code == 1 and json.loads(output.strip().splitlines()[-1]) == {"status": "rejected", "reason": "trusted_digest_differs"}, f"resealing passed: {output}"
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
        check_discriminator,
        check_scrub,
        check_scrub_mixed_possessive,
        check_scrub_text_words,
        check_scrub_apostrophe_forms,
        check_scrub_source_spellings,
        check_scrub_split_empty_markers,
        check_known_answer,
        check_isolation,
        check_sdk_logging,
        check_parameter_checks,
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
