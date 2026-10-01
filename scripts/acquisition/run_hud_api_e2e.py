"""Repeatable synthetic HUD capture-to-S3 verification, without live credentials."""

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

from scripts.acquisition import hud_api_contract as contract
from scripts.acquisition import hud_api_transport as transport
from scripts.acquisition import s3_store
from scripts.acquisition.build_hud_api_plan import build, build_range
from scripts.acquisition.collect_hud_api import execute, verify_local
from scripts.acquisition.run_bls_api_e2e import inventory
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, read_json, require
from scripts.acquisition.tests.test_s3_store import OUTPUTS, FakeS3

PRECISE = "0.123456789012"


def fixture(**changes: Any) -> dict:
    """Generic HUD-shaped rows: one ZIP per state, a split ZIP and a business-only ZIP."""
    rows = []
    for i, state in enumerate(contract.STATE_FIPS, 1):
        rows.append({"zip": f"{i:05d}", "geoid": f"{state}001", "res_ratio": 1, "bus_ratio": 1, "oth_ratio": 0, "tot_ratio": 1.0, "city": "SYN", "state": "ZZ"})
    split = rows[0]["zip"]
    rows[0] |= {"res_ratio": "SPLIT_A"}
    rows.append({"zip": split, "geoid": "01003", "res_ratio": "SPLIT_B", "bus_ratio": 0, "oth_ratio": 0, "tot_ratio": 0, "city": "SYN", "state": "ZZ"})
    rows.append({"zip": "99999", "geoid": "02013", "res_ratio": 0, "bus_ratio": 1, "oth_ratio": 0, "tot_ratio": 1, "city": "PO BOX", "state": "ZZ"})
    data = {"year": "2021", "quarter": "1", "input": "All", "crosswalk_type": "zip-county", "results": rows} | changes
    return {"data": data}


def encode(body: dict) -> bytes:
    """Serialize with exact decimal text for the split ZIP, as a publisher would."""
    text = json.dumps(body)
    return text.replace('"SPLIT_A"', PRECISE).replace('"SPLIT_B"', "0.876543210988").encode()


def exercise(root: Path) -> list[dict]:
    """Use production orchestration with fake HTTP and versioned storage boundaries."""
    plan = build() | {"request_spacing_seconds": 1}
    batch = plan["batches"][0]
    range_plan = build_range(plan)
    plan_path, range_path, versions = root / "plan.json", root / "range_plan.json", root / "versions.json"
    for path, locked in [(plan_path, plan), (range_path, range_plan)]:
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

    payload, calls, client = encode(fixture()), [], FakeS3()

    def request(*_args: Any) -> bytes:
        calls.append(1)
        return payload

    no_sleep = patch.object(transport.time, "sleep", lambda _: None)
    with (
        patch.object(contract, "PLAN_PATH", plan_path),
        patch.object(contract, "RANGE_PLAN_PATH", range_path),
        patch.object(contract, "VERSIONS_PATH", versions),
        no_sleep,
    ):
        scenario("locked_scope", contract.load_plan)
        run = root / "run"
        scenario("capture_without_storage", lambda: execute(plan, batch, run, True, False, client, OUTPUTS, request))
        receipt_path = run / "batches" / batch["id"] / "capture/receipt.json"
        scenario("offline_receipt_replay", lambda: verify_local(receipt_path))

        def precision() -> None:
            csv, stats = contract.validate_response(payload, batch, plan)
            require(PRECISE.encode() in csv and b",1.0,00002\n" in csv and b",01001," in csv, "Ratio text or leading zeros lost")
            require(stats["zips"] == len(contract.STATE_FIPS) + 1 and stats["rows"] == len(contract.STATE_FIPS) + 2, "One-to-many rows collapsed")
            res = stats["ratio_sums"]["res_ratio"]
            require(res["zips_with_zero_weight"] == 1 and res["zips_outside_tolerance"] == 0, "Residential weight profile differs")

        scenario("decimal_text_zeros_and_split_rows_kept", precision)
        scenario("capture_and_version_verified_storage", lambda: execute(plan, batch, run, True, True, client, OUTPUTS, request))
        before, objects = inventory(run), copy.deepcopy(client.objects)

        def repeat() -> None:
            execute(plan, batch, run, True, True, client, OUTPUTS, request)
            require(len(calls) == 1 and before == inventory(run) and objects == client.objects, "Repeated effects")

        scenario("repeat_one_no_effects", repeat)
        scenario("repeat_two_no_effects", repeat)

        def malformed(kind: str) -> None:
            body = fixture()
            rows = body["data"]["results"]
            if kind == "missing_state":
                body["data"]["results"] = [r for r in rows if not r["geoid"].startswith("56")]
            elif kind == "duplicate_row":
                rows.append(dict(rows[1]))
            elif kind == "extra_field":
                rows[1]["extra"] = "1"
            elif kind == "numeric_zip":
                rows[1]["zip"] = 2
            elif kind == "short_county":
                rows[1]["geoid"] = "1001"
            elif kind == "ratio_above_one":
                rows[1]["tot_ratio"] = 1.5
            elif kind == "string_ratio":
                rows[1]["tot_ratio"] = "1"
            elif kind == "wrong_quarter":
                body["data"]["quarter"] = "2"
            elif kind == "wrong_crosswalk":
                body["data"]["crosswalk_type"] = "county-zip"
            elif kind == "pagination_marker":
                body["data"]["next_page"] = 2
            elif kind == "no_envelope":
                body = body["data"]
            contract.validate_response(encode(body), batch, plan)

        for name, error in [
            ("missing_state", "state coverage"),
            ("duplicate_row", "Duplicate ZIP-county"),
            ("extra_field", "result fields"),
            ("numeric_zip", "text cell type"),
            ("short_county", "identifier invalid"),
            ("ratio_above_one", "ratio invalid"),
            ("string_ratio", "ratio invalid"),
            ("wrong_quarter", "period differs"),
            ("wrong_crosswalk", "scope differs"),
            ("pagination_marker", "envelope fields"),
            ("no_envelope", "envelope changed"),
        ]:
            scenario(name, lambda name=name: malformed(name), error)
        scenario("html_rejected", lambda: contract.validate_response(b"<h1>Login</h1>", batch, plan), "invalid JSON")

        def corrupt(kind: str) -> None:
            target = root / kind
            shutil.copytree(receipt_path.parent, target)
            receipt = read_json(target / "receipt.json")
            lineage = json.loads(receipt["lineage"]["extraction_or_query"])
            if kind == "raw_corruption":
                (target / "raw/response.json").write_bytes(b"{}")
            elif kind == "resealed_csv_corruption":
                p = target / "derived/crosswalk.csv"
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
            elif kind == "terms_binding":
                lineage["terms_sha256"] = "0" * 64
            elif kind == "attribution_removed":
                receipt["governance"]["use_restrictions"] = receipt["governance"]["use_restrictions"][1:]
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
            ("unreviewed_code", "Unreviewed HUD"),
            ("wrong_plan", "plan differs"),
            ("terms_binding", "terms binding"),
            ("attribution_removed", "governance differs"),
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
            transport.fetch(plan, batch, root / "retry", True, client, request=flaky, sleep=lambda _: None)
            require(len(attempts) == 3, "Retry bound or cache reuse differs")

        scenario("bounded_retries_and_cache_reuse", retry)

        def schema_change_cached() -> None:
            changed = root / "schema_change"
            with suppress(ValueError):
                execute(plan, batch, changed, True, False, client, OUTPUTS, lambda *_: encode(fixture(extra="x")))
            require((changed / "batches" / batch["id"] / "transport.json").exists(), "Raw response not cached for offline review")
            require(not (changed / "batches" / batch["id"] / "capture/receipt.json").exists(), "Receipt written for changed schema")

        scenario("schema_change_cached_without_receipt", schema_change_cached)

        def unreviewed_current() -> None:
            with patch.object(contract, "code_hashes", return_value={"changed": "0" * 64}):
                execute(plan, batch, root / "unreviewed", True, True, client, OUTPUTS, request)

        scenario("unreviewed_current_code_before_effects", unreviewed_current, "Unreviewed HUD")

        def tampered_plan() -> None:
            original = plan_path.read_bytes()
            try:
                plan_path.write_bytes(encoded_json({"changed": True}))
                contract.load_plan()
            finally:
                plan_path.write_bytes(original)

        scenario("plan_tampering_rejected", tampered_plan, "plan lock differs")

        def hold_gate(kind: str) -> None:
            source, lineage = {"source_id": "HUD"}, {"terms_sha256": plan["terms"]["sha256"]}
            if kind == "stale_terms_hash":
                lineage["terms_sha256"] = "0" * 64
            elif kind == "source_without_acceptance":
                source = {"source_id": "AHA"}
            elif kind == "missing_binding":
                lineage = {}
            require(not s3_store.access_released(source, lineage), "Access hold released without matching evidence")

        scenario(
            "hold_released_by_bound_terms",
            lambda: require(s3_store.access_released({"source_id": "HUD"}, {"terms_sha256": plan["terms"]["sha256"]}), "Gate refused"),
        )
        for kind in ["stale_terms_hash", "source_without_acceptance", "missing_binding"]:
            scenario(f"hold_kept_{kind}", lambda kind=kind: hold_gate(kind))
        scenario("secret_transport_controls", lambda: secret_checks(plan, batch, payload, root / "secret"))
        range_scenarios(scenario, plan, range_plan, range_path, receipt_path, run, client, root)
    return scenarios


def range_scenarios(scenario: Any, plan: dict, range_plan: dict, range_path: Path, pilot_receipt: Path, pilot_run: Path, client: FakeS3, root: Path) -> None:
    """The 2021 Q2-2025 Q4 plan supplements the pilot; the stored pilot must stay valid unchanged."""
    batches = {(b["year"], b["quarter"]): b for b in range_plan["batches"]}
    q4 = batches[(2023, 4)]
    q4_payload, q4_calls = encode(fixture(year="2023", quarter="4")), []

    def q4_request(*_args: Any) -> bytes:
        q4_calls.append(1)
        return q4_payload

    scenario("range_plan_locked_scope", lambda: contract.load_plan(range_path))
    scenario(
        "range_has_19_quarters", lambda: require(len(range_plan["batches"]) == 19 and min(batches) == (2021, 2) and max(batches) == (2025, 4), "Range differs")
    )
    scenario("plans_chain", lambda: require(len(contract.load_plans()) == 2, "Plan chain incomplete"))
    eras = {k: b["county_geography"] for k, b in batches.items()}
    scenario("geography_era_boundary", lambda: require(eras[(2022, 4)] == "2010_census" and eras[(2023, 1)] == "2020_census", "Era boundary differs"))
    run = root / "range_run"
    scenario("q4_capture_and_version_verified_storage", lambda: execute(range_plan, q4, run, True, True, client, OUTPUTS, q4_request))
    receipt_path = run / "batches" / q4["id"] / "capture/receipt.json"

    def q4_period() -> None:
        receipt = verify_local(receipt_path)
        period = receipt["measurement_periods"][0]
        require((period["start_date"], period["end_date"]) == ("2023-10-01", "2023-12-31"), "Quarter dates differ")
        require(json.loads(receipt["lineage"]["extraction_or_query"])["county_geography"] == "2020_census", "Geography era missing")

    scenario("q4_dates_and_era_recorded", q4_period)
    before, objects = inventory(run), copy.deepcopy(client.objects)

    def repeat() -> None:
        execute(range_plan, q4, run, True, True, client, OUTPUTS, q4_request)
        require(len(q4_calls) == 1 and before == inventory(run) and objects == client.objects, "Repeated range effects")

    scenario("range_repeat_no_effects", repeat)

    def pilot_unchanged() -> None:
        pilot_before = inventory(pilot_run)
        verify_local(pilot_receipt)
        execute(plan, plan["batches"][0], pilot_run, False, True, client, OUTPUTS)
        require(pilot_before == inventory(pilot_run) and objects == client.objects, "Pilot replay changed effects")

    scenario("pilot_valid_beside_range_plan", pilot_unchanged)
    scenario("q1_2021_response_for_q4_rejected", lambda: contract.validate_response(encode(fixture()), q4, range_plan), "period differs")
    scenario(
        "range_batch_under_pilot_plan_rejected", lambda: execute(plan, q4, root / "cross", True, True, client, OUTPUTS, q4_request), "selected plan or batch"
    )

    def variant(kind: str) -> None:
        body = copy.deepcopy(range_plan)
        if kind == "extra_2026_quarter":
            extra = {"type": 2, "query": "All", "year": 2026, "quarter": 1}
            body["batches"].append(extra | {"county_geography": "2020_census", "id": contract.batch_id(extra)})
        elif kind == "wrong_era":
            body["batches"][0]["county_geography"] = "2020_census"
        elif kind == "broken_chain":
            body["supplements_plan_sha256"] = "0" * 64
        path = root / f"range_{kind}.json"
        write_once(path, encoded_json(body))
        write_once(path.with_suffix(".lock.json"), encoded_json({"plan_sha256": canonical_hash(body)}))
        if kind == "broken_chain":
            with patch.object(contract, "RANGE_PLAN_PATH", path):
                contract.load_plans()
        else:
            contract.load_plan(path)

    for name, error in [("extra_2026_quarter", "quarters differ"), ("wrong_era", "geography era"), ("broken_chain", "plan chain differs")]:
        scenario(f"range_{name}_rejected", lambda name=name: variant(name), error)

    def claims_other_period() -> None:
        target = root / "wrong_period"
        shutil.copytree(receipt_path.parent, target)
        receipt = read_json(target / "receipt.json")
        receipt["measurement_periods"][0].update(start_date="2023-07-01", end_date="2023-09-30")
        (target / "receipt.json").write_bytes(encoded_json(receipt))
        verify_local(target / "receipt.json")

    scenario("receipt_with_wrong_quarter_dates_rejected", claims_other_period, "measurement quarter differs")
    layout_scenarios(scenario, range_plan, batches[(2022, 1)], client, root)
    territory_scenarios(scenario, range_plan, batches[(2024, 2)], client, root)


def without(body: dict, names: tuple[str, ...], rows: slice = slice(None)) -> dict:
    """Drop fields from the selected result rows."""
    for row in body["data"]["results"][rows]:
        for name in names:
            row.pop(name, None)
    return body


def layout_scenarios(scenario: Any, plan: dict, batch: dict, client: FakeS3, root: Path) -> None:
    """From 2022 Q1 HUD omits city and state; accept only that whole-response layout."""
    body = without(fixture(year="2022", quarter="1"), contract.OPTIONAL_FIELDS)
    payload, run = encode(body), root / "layout_run"
    scenario("no_city_state_capture_and_storage", lambda: execute(plan, batch, run, True, True, client, OUTPUTS, lambda *_: payload))

    def recorded() -> None:
        receipt = verify_local(run / "batches" / batch["id"] / "capture/receipt.json")
        headers = receipt["schema_profile"]["native_headers"]
        require("city" not in headers and "state" not in headers and "zip" in headers, "Layout headers differ")
        require(any("omitted city, state" in w for w in receipt["quality_profile"]["warnings"]), "Omission not recorded")
        csv = (run / "batches" / batch["id"] / "capture/derived/crosswalk.csv").read_bytes()
        require(csv.startswith(b"bus_ratio,geoid,oth_ratio,res_ratio,tot_ratio,zip\n"), "Absent fields filled in")

    scenario("omission_recorded_not_filled", recorded)
    mixed = encode(without(fixture(year="2022", quarter="1"), contract.OPTIONAL_FIELDS, slice(1, None)))
    scenario("mixed_layout_rejected", lambda: contract.validate_response(mixed, batch, plan), "result fields differ")
    only_city = encode(without(fixture(year="2022", quarter="1"), ("state",)))
    scenario("partial_optional_layout_rejected", lambda: contract.validate_response(only_city, batch, plan), "result fields differ")
    no_ratio = encode(without(fixture(year="2022", quarter="1"), ("city", "state", "bus_ratio")))
    scenario("missing_ratio_still_rejected", lambda: contract.validate_response(no_ratio, batch, plan), "result fields differ")


def secret_checks(plan: dict, batch: dict, payload: bytes, root: Path) -> None:
    """Token only in the Authorization header; reject redirects, HTML, echoes and wrong identities."""
    canary = "SYNTHETIC.HUD-TOKEN_FOR.E2E.ONLY"
    client = FakeS3()
    client.overrides["get-secret-value"] = {"SecretString": json.dumps({"api_key": canary})}

    class Connection:
        status, content_type, body = 200, "application/json", payload

        def request(self, method: str, path: str, headers: dict) -> None:
            require(method == "GET" and canary not in path and headers.get("Authorization") == f"Bearer {canary}", "Credential not sent as header only")

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
        execute(plan, batch, root, True, False, client, OUTPUTS)
        require(all(canary.encode() not in p.read_bytes() for p in root.rglob("*") if p.is_file()), "Secret persisted locally")
        for kind in ["redirect", "html", "echo", "identity", "malformed_secret"]:
            connection.status, connection.content_type, connection.body = 200, "application/json", payload
            count = sum(c[0] == "get-secret-value" for c in client.calls)
            if kind == "redirect":
                connection.status = 302
            elif kind == "html":
                connection.content_type, connection.body = "text/html", b"<h1>Login</h1>"
            elif kind == "echo":
                connection.body = json.dumps({"data": canary}).encode()
            elif kind == "malformed_secret":
                client.overrides["get-secret-value"] = {"SecretString": json.dumps({"api_key": "Bearer " + canary})}
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


def with_geoid(geoid: str, drop: tuple[str, ...] = contract.OPTIONAL_FIELDS) -> dict:
    """2024 Q2-shaped response plus one Pacific ZIP under the given area code."""
    body = without(fixture(year="2024", quarter="2"), drop)
    body["data"]["results"].append({"zip": "96941", "geoid": geoid, "res_ratio": 0, "bus_ratio": 1, "oth_ratio": 0, "tot_ratio": 1})
    return body


def territory_scenarios(scenario: Any, plan: dict, batch: dict, client: FakeS3, root: Path) -> None:
    """Only the four approved territory codes may appear without a county; kept as published."""
    run = root / "territory_run"
    payload = encode(with_geoid("64"))
    scenario("territory_code_capture_and_storage", lambda: execute(plan, batch, run, True, True, client, OUTPUTS, lambda *_: payload))

    def recorded() -> None:
        receipt = verify_local(run / "batches" / batch["id"] / "capture/receipt.json")
        require(any(w.startswith("1 rows use a two-digit territory") for w in receipt["quality_profile"]["warnings"]), "Territory rows not recorded")
        csv = (run / "batches" / batch["id"] / "capture/derived/crosswalk.csv").read_bytes()
        require(b",64," in csv and b"00064" not in csv and b"64000" not in csv, "Territory code altered")
        _, stats = contract.validate_response(payload, batch, plan)
        require(
            "64" in stats["states_and_territories"] and stats["counties"] == len({r["geoid"] for r in fixture()["data"]["results"]}),
            "Territory counted as county",
        )

    scenario("territory_rows_recorded_not_padded", recorded)
    for code in ["99", "66", "064"]:
        body = encode(with_geoid(code))
        scenario(f"unapproved_area_code_{code}_rejected", lambda body=body: contract.validate_response(body, batch, plan), "identifier invalid")


def main() -> None:
    """Write one immutable artifact for every E2E invocation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="hud_api_e2e_") as directory:
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
