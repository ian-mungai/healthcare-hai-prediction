"""Build a publisher-evidenced plan for pre-table-based ACS five-year files."""

import argparse
import csv
import hashlib
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

from scripts.acquisition.discover_history import candidate
from scripts.acquisition.discover_history import index as publisher_index
from scripts.acquisition.s3_store import encoded_json, fingerprint, write_once
from scripts.acquisition.source_registry import canonical_hash, load_registry, read_json
from scripts.acquisition.transport import Limits, download

ROOT = Path("data/historical_acquisition/acs_sequence_discovery")


def index(url: str, root: Path) -> tuple:
    """Return publisher-index links and their preserved retrieval evidence."""
    saved = root / "indexes" / hashlib.sha256(url.encode()).hexdigest() / "index.json"
    if saved.exists() and read_json(saved)["response"]["failure"]:
        root = root / "retry_01"
        saved = root / "indexes" / hashlib.sha256(url.encode()).hexdigest() / "index.json"
    if not saved.exists():
        time.sleep(2)
    links, evidence = publisher_index(url, root)
    if evidence["response"]["failure"]:
        raise ValueError(f"Publisher index unavailable: {evidence['response']['failure']}; {url}")
    return links, evidence


def discover_year(year: int) -> dict:
    """Return publisher-evidenced ACS sequence candidates and unresolved coverage failures for a year."""
    root = ROOT / str(year)
    base = f"https://www2.census.gov/programs-surveys/acs/summary_file/{year}/"
    fields = " ".join(m["preserved_controls"].get("current_exact_field") or "" for m in load_registry()["measure_controls"])
    tables = set(re.findall(r"\b(?:B\d{5}[A-I]?|C\d{5})(?=[_\s;,.])", fields))
    candidates, documents, failures = {}, [], []
    links, _ = index(base, root)
    data = next(x["url"] for x in links if x["label"] == "data/")
    docs = next(x["url"] for x in links if x["label"] == "documentation/")
    queue = [(docs, 0)]
    while queue:
        url, depth = queue.pop(0)
        children, evidence = index(url, root)
        for item in children:
            name = Path(urlsplit(item["url"]).path).name.lower()
            if item["label"] in {"5_year/", "user_tools/"} and item["url"].startswith(url) and depth < 2:
                queue.append((item["url"], depth + 1))
            elif "lookup" in name and ("5yr" in name or "/5_year/" in item["url"]) and name.endswith(".txt"):
                documents.append((item, evidence))
    if len(documents) != 1:
        return {"year": year, "status": "lookup_selection_required", "lookup_count": len(documents), "candidates": []}
    item, evidence = documents[0]
    receipt_path = root / "lookup_receipt.json"
    if receipt_path.exists():
        record = read_json(receipt_path)
        path = Path(record["path"])
        if record["url"] != item["url"] or fingerprint(path)[0] != record["sha256"]:
            raise ValueError("Cached lookup URL or SHA-256 differs from recorded evidence.")
    else:
        directories = [root / "lookup", root / "lookup_retry_01", root / "lookup_retry_02"]
        directory = next((entry for entry in directories if not entry.exists()), None)
        if directory is None:
            raise ValueError("Lookup attempts exhausted; prior evidence retained for review.")
        result = download(item["url"], directory, "lookup.txt", "txt", "layout", Limits(max_bytes=8 * 1024**2, attempts=1))
        if result.failure:
            return {"year": year, "status": result.failure, "candidates": []}
        path = result.path
        write_once(receipt_path, encoded_json({"path": str(path), "sha256": result.sha256, "url": item["url"]}))
    try:
        path.read_text(encoding="utf-8-sig")
        encoding = "utf-8-sig"
    except UnicodeDecodeError:
        encoding = "cp1252"
    with path.open(encoding=encoding) as handle:
        rows = list(csv.DictReader(handle))
    mappings = {table: sorted({row["Sequence Number"].strip().zfill(4) for row in rows if row["Table ID"] == table}) for table in tables}
    sequences = {seq for values in mappings.values() for seq in values}
    if not sequences or any(not re.fullmatch(r"\d{4}", seq) for seq in sequences):
        raise ValueError("Invalid or missing publisher table-to-sequence mapping.")
    row = candidate("ACS", item, evidence, 8 * 1024**2)
    row["role"] = "reference"
    row["expected_sha256"] = read_json(receipt_path)["sha256"]
    candidates[row["url"]] = row
    children, data_evidence = index(data, root)
    for item in children:
        if "5yr_summary_filetemplates" in item["label"].lower() and item["url"].endswith(".zip"):
            row = candidate("ACS", item, data_evidence, 8 * 1024**2)
            row["role"] = "reference"
            candidates[row["url"]] = row
    sequence_root = next(x["url"] for x in children if x["label"] == "5_year_seq_by_state/")
    states, _ = index(sequence_root, root)
    states = [item for item in states if item["url"].startswith(sequence_root) and item["url"] != sequence_root and item["url"].endswith("/")]
    for state in states:
        children, _ = index(state["url"], root)
        area = next((x["url"] for x in children if x["label"] == "All_Geographies_Not_Tracts_Block_Groups/"), None)
        if area is None:
            failures.append({"state": state["label"], "reason": "county_inclusive_directory_missing"})
            continue
        files, file_evidence = index(area, root)
        found, geographies, state_candidates = set(), 0, {}
        for item in files:
            name = Path(urlsplit(item["url"]).path).name
            match = re.fullmatch(str(year) + r"5[a-z]{2}(\d{4})000\.zip", name)
            geo = re.fullmatch("g" + str(year) + r"5[a-z]{2}\.(?:csv|txt)", name)
            if not (match and match[1] in sequences) and not geo:
                continue
            row = candidate("ACS", item, file_evidence, 16 * 1024**2)
            row["scope"] = f"Complete publisher file for ACS {year} five-year window; {state['label']}; county-inclusive geography, not a county-only extract."
            row["evidence"].update(lookup_sha256=read_json(receipt_path)["sha256"], table_sequences=mappings)
            if geo:
                row["role"] = "reference"
                geographies += 1
            elif match is not None:
                found.add(match[1])
            state_candidates[row["url"]] = row
        if found != sequences or geographies == 0:
            failures.append({"state": state["label"], "missing_sequences": sorted(sequences - found), "geography_files": geographies})
        else:
            candidates.update(state_candidates)
    return {
        "year": year,
        "lookup_encoding": encoding,
        "table_sequences": mappings,
        "states_listed": len(states),
        "failures": failures,
        "unavailable_tables": sorted(table for table, seqs in mappings.items() if not seqs),
        "status": "discovery_incomplete" if failures else "planned_not_captured",
        "candidates": list(candidates.values()),
    }


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", nargs="+", type=int, choices=range(2009, 2018), default=list(range(2009, 2018)))
    args = parser.parse_args()
    years = []
    for year in args.years:
        try:
            result = discover_year(year)
        except (ValueError, StopIteration, KeyError) as error:
            result = {"year": year, "status": "discovery_blocked", "reason": str(error), "candidates": []}
        years.append(result)
        if result["status"] == "discovery_blocked":
            break
    candidates = [row for year in years for row in year["candidates"]]
    output = {
        "years": years,
        "candidates": candidates,
        "model_eligible": False,
        "plan_sha256": canonical_hash(candidates),
        "plan_status": "discovery_incomplete"
        if any(year["status"] != "planned_not_captured" for year in years)
        else "publisher_links_and_lookup_verified_not_downloaded",
    }
    write_once(ROOT / ("candidates_" + canonical_hash(output)[:16] + ".json"), encoded_json(output))
    sys.stdout.write(str([(year["year"], year["status"], len(year["candidates"]), year.get("failures")) for year in years]) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
