"""Repeatable synthetic Census capture-to-S3 verification, without live credentials."""

import argparse
import copy
import json
import shutil
import sys
import tempfile
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import patch

from scripts.acquisition import census_acs_api_contract as contract
from scripts.acquisition import census_acs_api_transport as transport
from scripts.acquisition.collect_census_acs_api import execute, verify_local
from scripts.acquisition.run_bls_api_e2e import inventory
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, load_registry, read_json, require
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3


def fixture(batch: dict, counties: tuple[str, ...] = ("01001", "72001")) -> bytes:
    """Generic public-shaped tokens, including null, empty annotation and a sentinel."""
    rows = [batch["headers"]]
    for county in counties:
        values: dict[str, Any] = dict.fromkeys(batch["headers"], "1")
        values.update(NAME="Synthetic county", GEO_ID=f"0500000US{county}", state=county[:2], county=county[2:])
        values[f"{batch['table']}_0001EA"] = None
        values[f"{batch['table']}_0001MA"] = ""
        values[f"{batch['table']}_0001PE"] = "-888888888"
        rows.append([values[h] for h in batch["headers"]])
    return json.dumps(rows).encode()


def exercise(root: Path) -> list[dict]:
    """Use production orchestration with fake HTTP and versioned storage boundaries."""
    batches: list[dict] = []
    references: list[dict] = []
    for table in contract.TABLES + contract.PR_TABLES:
        fields = [f"{table}_0001{s}" for s in ("E", "M", "PE", "PM", "EA", "MA", "PEA", "PMA")] + ["GEO_ID", "NAME"]
        body = encoded_json({"variables": {v: {"label": "Synthetic variable"} for v in fields}})
        path = root / f"{table}.json"
        write_once(path, body)
        ref = {"path": str(path), "sha256": contract.digest(body), "url": f"{contract.ENDPOINT}/groups/{table}.json"}
        references.append(ref)
        batch: dict = {"table": table, "headers": sorted(fields + ["state", "county"]), "metadata": ref}
        batches.append(batch | {"id": contract.batch_id(batch)})
    pr_references, pr_batches = references[len(contract.TABLES) :], batches[len(contract.TABLES) :]
    references, batches = references[: len(contract.TABLES)], batches[: len(contract.TABLES)]
    plan: dict = {
        "version": 1,
        "source_id": "ACS",
        "endpoint": contract.ENDPOINT,
        "credential_reference": "census_api_key",
        "year": 2009,
        "window": [2005, 2009],
        "vintage": "synthetic",
        "county_ids": ["01001", "72001"],
        "references": references,
        "batches": batches,
        "model_eligible": False,
        "registry_sha256": canonical_hash(load_registry()),
        "request_spacing_seconds": 0,
    }
    pr_plan: dict = plan | {
        "version": 2,
        "county_ids": ["72001"],
        "references": pr_references,
        "batches": pr_batches,
        "supplements_plan_sha256": canonical_hash(plan),
        "scope": "Synthetic DP02PR scope",
    }
    plan_path, pr_plan_path, versions = root / "plan.json", root / "plan_pr.json", root / "versions.json"
    for path, locked in [(plan_path, plan), (pr_plan_path, pr_plan)]:
        write_once(path, encoded_json(locked))
        write_once(path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(locked)}))
    write_once(versions, encoded_json({"versions": [{"code_sha256": contract.code_hashes()}]}))
    scenarios: list[dict] = []

    def scenario(name: str, action: Any, error: str | None = None) -> None:
        try:
            action()
            scenarios.append({"name": name, "passed": error is None, "observed": "success", "expected": error or "success"})
        except (ValueError, OSError, KeyError, TypeError, StopIteration) as failure:
            scenarios.append(
                {"name": name, "passed": error is not None and error in str(failure), "observed": str(failure)[:200], "expected": error or "success"}
            )

    batch, other = batches[:2]
    payload, calls, client = fixture(batch), [], FakeS3()

    def request(*_args: Any) -> bytes:
        calls.append(1)
        return payload

    with (
        patch.object(contract, "PLAN_PATH", plan_path),
        patch.object(contract, "PR_PLAN_PATH", pr_plan_path),
        patch.object(contract, "VERSIONS_PATH", versions),
    ):
        scenario("locked_scope", contract.load_plan)
        run = root / "run"
        scenario("capture_and_version_verified_storage", lambda: execute(plan, batch, run, True, True, client, OUTPUTS, request))
        before, objects = inventory(run), copy.deepcopy(client.objects)
        receipt_path = run / "batches" / batch["id"] / "capture/receipt.json"

        def repeat() -> None:
            execute(plan, batch, run, True, True, client, OUTPUTS, request)
            require(len(calls) == 1 and before == inventory(run) and objects == client.objects, "Repeated effects")

        scenario("repeat_one_no_effects", repeat)
        scenario("repeat_two_no_effects", repeat)
        scenario("offline_receipt_replay", lambda: verify_local(receipt_path))

        def preserve_tokens() -> None:
            csv, stats = contract.validate_response(payload, batch, plan["county_ids"])
            require(b"\\N" in csv and b"-888888888" in csv and stats["null_cells"] == 2, "Native tokens lost")

        scenario("null_empty_and_sentinel_distinct", preserve_tokens)

        def malformed(kind: str) -> None:
            rows = json.loads(payload)
            if kind == "missing_county":
                rows.pop()
            elif kind == "duplicate_county":
                rows.append(rows[1])
            elif kind == "missing_field":
                rows = [r[:-1] for r in rows]
            elif kind == "duplicate_header":
                rows[0][-1] = rows[0][0]
            elif kind == "row_width":
                rows[1].pop()
            elif kind == "numeric_cell":
                rows[1][0] = 1
            elif kind == "wrong_geo_id":
                rows[1][rows[0].index("GEO_ID")] = "0500000US99999"
            elif kind == "null_marker_collision":
                rows[1][0] = "\\N"
            contract.validate_response(encoded_json(rows), batch, plan["county_ids"])

        for name, error in [
            ("missing_county", "county coverage"),
            ("duplicate_county", "Duplicate county"),
            ("missing_field", "headers"),
            ("duplicate_header", "headers"),
            ("row_width", "row width"),
            ("numeric_cell", "native cell"),
            ("wrong_geo_id", "geography"),
            ("null_marker_collision", "null marker"),
        ]:
            scenario(name, lambda name=name: malformed(name), error)
        scenario("html_rejected", lambda: contract.validate_response(b"<h1>Missing Key</h1>", batch, plan["county_ids"]), "JSON")

        def corrupt(kind: str) -> None:
            target = root / kind
            shutil.copytree(receipt_path.parent, target)
            receipt = read_json(target / "receipt.json")
            lineage = json.loads(receipt["lineage"]["extraction_or_query"])
            if kind == "raw_corruption":
                (target / "raw/response.json").write_bytes(b"[]")
            elif kind == "resealed_csv_corruption":
                p = target / "derived/observations.csv"
                p.write_bytes(p.read_bytes() + b"extra\n")
                for a in receipt["artifacts"]:
                    if a["role"] == "data":
                        a["sha256"], a["byte_count"] = fingerprint(p)
            elif kind == "model_promotion":
                lineage["model_eligible"] = True
            elif kind == "unreviewed_code":
                lineage["code_sha256"] = {"bad": "0" * 64}
            elif kind == "wrong_plan":
                lineage["plan_sha256"] = "0" * 64
            elif kind == "resealed_transport_proof":
                path = target / "evidence/request_response.json"
                proof = read_json(path)
                proof.update(sha256="0" * 64, bytes=1, http_status=302)
                path.write_bytes(encoded_json(proof))
                for item in receipt["artifacts"]:
                    if item["role"] == "export_receipt":
                        item["sha256"], item["byte_count"] = fingerprint(path)
            receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
            (target / "receipt.json").write_bytes(encoded_json(receipt))
            verify_local(target / "receipt.json")

        for name, error in [
            ("raw_corruption", "Artifact hash"),
            ("resealed_csv_corruption", "CSV differs"),
            ("model_promotion", "hold differs"),
            ("unreviewed_code", "Unreviewed Census"),
            ("wrong_plan", "plan differs"),
            ("resealed_transport_proof", "transport proof differs"),
        ]:
            scenario(name, lambda name=name: corrupt(name), error)

        def interrupted() -> None:
            partial, s3 = root / "partial", FakeS3()
            s3.corrupt = True
            with suppress(ValueError):
                execute(plan, batch, partial, True, True, s3, OUTPUTS, request)
            require(len(s3.objects) == 1, "No interrupted upload")
            n = len(calls)
            s3.corrupt = False
            execute(plan, batch, partial, False, True, s3, OUTPUTS)
            require(len(calls) == n and len(s3.objects) == len(objects), "Resume duplicated effects")

        scenario("resume_interrupted_storage", interrupted)

        def swapped() -> None:
            shutil.copytree(receipt_path.parent, root / "swapped/batches" / other["id"] / "capture")
            execute(plan, other, root / "swapped", False, True, client, OUTPUTS)

        scenario("wrong_batch_capture", swapped, "selected batch")

        def invalid_version() -> None:
            target = root / "bad_storage"
            shutil.copytree(run, target)
            branch = target / "batches" / batch["id"]
            path = branch / "capture/s3_collections_reconciliation.json"
            rec = read_json(path)
            rec["objects"][0]["object"]["version_id"] = ""
            path.write_bytes(encoded_json(rec))
            done = read_json(branch / "completed.json")
            done["reconciliation_sha256"] = fingerprint(path)[0]
            (branch / "completed.json").write_bytes(encoded_json(done))
            execute(plan, batch, target, False, True, client, OUTPUTS)

        scenario("resealed_missing_storage_version", invalid_version, "version missing")

        def concurrency() -> None:
            with transport.collection_lock(root / "lock"), transport.collection_lock(root / "lock"):
                raise ValueError("Lock failed")

        scenario("concurrent_run_rejected", concurrency, "already running")

        def retry() -> None:
            attempts: list[int] = []

            def flaky(*_args: Any) -> bytes:
                attempts.append(1)
                if len(attempts) < 3:
                    raise transport.RetryableRequest("Synthetic transient failure")
                return payload

            transport.fetch(plan, batch, root / "retry", True, client, request=flaky, sleep=lambda _: None)
            require(len(attempts) == 3, "Retry bound differs")
            transport.fetch(plan, batch, root / "retry", True, client, request=flaky, sleep=lambda _: None)
            require(len(attempts) == 3, "Cached response fetched again")

        scenario("bounded_retries_and_cache_reuse", retry)

        def unreviewed_current() -> None:
            with patch.object(contract, "code_hashes", return_value={"changed": "0" * 64}):
                execute(plan, batch, root / "unreviewed", True, True, client, OUTPUTS, request)

        scenario("unreviewed_current_code_before_effects", unreviewed_current, "Unreviewed Census")

        def changed_input(kind: str) -> None:
            path = plan_path if kind == "plan" else Path(batch["metadata"]["path"])
            original = path.read_bytes()
            try:
                path.write_bytes(encoded_json({"changed": True}) if kind == "plan" else b"{}")
                contract.load_plan()
            finally:
                path.write_bytes(original)

        scenario("plan_tampering_rejected", lambda: changed_input("plan"), "plan lock differs")
        scenario("dictionary_tampering_rejected", lambda: changed_input("dictionary"), "reference changed")
        scenario("secret_transport_controls", lambda: secret_checks(plan, batch, payload, root / "secret"))
        pr_scenarios(scenario, plan, pr_plan, pr_plan_path, receipt_path, run, client, root)
    return scenarios


def pr_scenarios(scenario: Any, plan: dict, pr_plan: dict, pr_plan_path: Path, v1_receipt: Path, v1_run: Path, client: FakeS3, root: Path) -> None:
    """DP02PR lives in a second locked plan; the stored v1 captures must stay valid unchanged."""
    pr_batch, pr_payload, pr_calls = pr_plan["batches"][0], fixture(pr_plan["batches"][0], ("72001",)), []

    def pr_request(*_args: Any) -> bytes:
        pr_calls.append(1)
        return pr_payload

    scenario("pr_plan_locked_scope", lambda: contract.load_plan(pr_plan_path))
    scenario("both_plans_chain", lambda: require(len(contract.load_plans()) == 2, "Plan chain incomplete"))
    expected_request = {"get": "group(DP02PR)", "for": "county:*", "in": "state:72"}
    scenario("pr_request_puerto_rico_only", lambda: require(contract.request_for(pr_batch) == expected_request, "PR request scope differs"))
    pr_run = root / "pr_run"
    scenario("pr_capture_and_version_verified_storage", lambda: execute(pr_plan, pr_batch, pr_run, True, True, client, OUTPUTS, pr_request))
    before, objects = inventory(pr_run), copy.deepcopy(client.objects)

    def pr_repeat() -> None:
        execute(pr_plan, pr_batch, pr_run, True, True, client, OUTPUTS, pr_request)
        require(len(pr_calls) == 1 and before == inventory(pr_run) and objects == client.objects, "Repeated PR effects")

    scenario("pr_repeat_one_no_effects", pr_repeat)
    scenario("pr_repeat_two_no_effects", pr_repeat)

    def v1_unchanged() -> None:
        v1_before = inventory(v1_run)
        verify_local(v1_receipt)
        execute(plan, plan["batches"][0], v1_run, False, True, client, OUTPUTS)
        require(v1_before == inventory(v1_run) and objects == client.objects, "v1 replay changed effects")

    scenario("v1_capture_valid_beside_pr_plan", v1_unchanged)
    us_rows = fixture(pr_batch, ("01001", "72001"))
    scenario("pr_response_with_us_county_rejected", lambda: contract.validate_response(us_rows, pr_batch, pr_plan["county_ids"]), "county coverage")
    scenario(
        "pr_batch_under_v1_plan_rejected", lambda: execute(plan, pr_batch, root / "cross", True, True, client, OUTPUTS, pr_request), "selected plan or batch"
    )

    def variant(kind: str) -> None:
        body = copy.deepcopy(pr_plan)
        if kind == "us_county":
            body["county_ids"] = ["01001", "72001"]
        elif kind == "v1_table_in_pr_plan":
            body["batches"] = [plan["batches"][0]]
        elif kind == "pr_table_in_v1_plan":
            body = copy.deepcopy(plan)
            body["batches"] = [*plan["batches"], pr_batch]
        path = root / f"variant_{kind}.json"
        write_once(path, encoded_json(body))
        write_once(path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(body)}))
        contract.load_plan(path)

    for name, error in [
        ("us_county", "Puerto Rico plan scope"),
        ("v1_table_in_pr_plan", "table scope differs"),
        ("pr_table_in_v1_plan", "table scope differs"),
    ]:
        scenario(f"{name}_rejected", lambda name=name: variant(name), error)

    def broken_chain() -> None:
        body = pr_plan | {"supplements_plan_sha256": "0" * 64}
        path = root / "variant_broken_chain.json"
        write_once(path, encoded_json(body))
        write_once(path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(body)}))
        with patch.object(contract, "PR_PLAN_PATH", path):
            contract.load_plans()

    scenario("pr_plan_bound_to_other_v1_rejected", broken_chain, "plan chain differs")

    def v1_capture_claims_pr_plan() -> None:
        target = root / "claims_pr_plan"
        shutil.copytree(v1_receipt.parent, target)
        receipt = read_json(target / "receipt.json")
        lineage = json.loads(receipt["lineage"]["extraction_or_query"])
        lineage["plan_sha256"] = canonical_hash(pr_plan)
        receipt["lineage"]["extraction_or_query"] = json.dumps(lineage)
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    scenario("v1_capture_claiming_pr_plan_rejected", v1_capture_claims_pr_plan, "batch not in its plan")
    scenario("pr_secret_transport_controls", lambda: secret_checks(pr_plan, pr_batch, pr_payload, root / "pr_secret"))


def secret_checks(plan: dict, batch: dict, payload: bytes, root: Path) -> None:
    """Reject HTML, redirects, credential echoes and wrong identities without logging secrets."""
    canary = "SYNTHETICCENSUSKEYFORE2E"
    client = FakeS3()
    client.overrides["get-secret-value"] = {"SecretString": json.dumps({"api_key": canary})}

    class Connection:
        status, content_type, body = 200, "application/json", payload

        def request(self, method: str, path: str, headers: dict) -> None:
            require(method == "GET" and f"key={canary}" in path, "Credential not passed at runtime")

        def getresponse(self) -> Any:
            return self

        def getheader(self, _name: str, _default: str) -> str:
            return self.content_type

        def read(self, _size: int) -> bytes:
            return self.body

        def close(self) -> None:
            return

    connection = Connection()
    with patch.object(transport.http.client, "HTTPSConnection", return_value=connection):
        execute(plan, batch, root, True, True, client, OUTPUTS)
        require(all(canary.encode() not in p.read_bytes() for p in root.rglob("*") if p.is_file()), "Secret persisted locally")
        require(all(canary.encode() not in o["body"] for o in client.objects.values()), "Secret persisted in S3")
        for kind in ["redirect", "html", "echo", "identity"]:
            connection.status, connection.content_type, connection.body = 200, "application/json", payload
            count = sum(c[0] == "get-secret-value" for c in client.calls)
            if kind == "redirect":
                connection.status = 302
            elif kind == "html":
                connection.content_type, connection.body = "text/html", b"<h1>Missing Key</h1>"
            elif kind == "echo":
                connection.body = json.dumps([["echo"], [canary]]).encode()
            else:
                client.overrides["get-caller-identity"] = {"Account": "wrong", "Arn": "wrong"}
            try:
                transport.request_live(plan, batch, client)
            except ValueError as error:
                require(canary not in str(error), "Unsafe credential error")
            else:
                raise ValueError(f"Accepted unsafe {kind}")
            if kind == "identity":
                require(sum(c[0] == "get-secret-value" for c in client.calls) == count, "Secret read before identity")


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="census_acs_e2e_") as directory:
        scenarios = exercise(Path(directory))
    passed = all(s["passed"] for s in scenarios)
    report = {
        "status": "passed" if passed else "failed",
        "run_at_utc": datetime.now(UTC).isoformat(),
        "code_sha256": contract.code_hashes(),
        "dependency_sha256": contract.digest(Path("requirements.txt").read_bytes()),
        "scenarios": scenarios,
    }
    write_once(args.output, encoded_json(report))
    sys.stdout.write(
        json.dumps({"status": report["status"], "passed": sum(s["passed"] for s in scenarios), "total": len(scenarios), "artifact": str(args.output)}) + "\n"
    )
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
