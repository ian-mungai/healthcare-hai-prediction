"""Compare the extracted key sets across sources: county universes, hospital coverage, ZIP-to-county routes and time.

Reads the ``keysets.json`` written by ``extract_cross_source_keys`` and writes one deterministic report. Every
comparison is a set operation on public identifiers within a stated period; nothing is joined to row values and
no join rule is chosen.

Usage::

    python -m scripts.review.analyze_cross_source_keys --keysets keys_run1/keysets.json --output cross_source_run1.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from scripts.review.review_remaining import digest, write_json

HAI = "main-hai-pdc"
STATES = {f"{n:02d}" for n in range(1, 57)} - {"03", "07", "14", "43", "52"}
TERRITORIES = {"60", "64", "66", "68", "69", "70", "74", "78"}
CT_LEGACY = {f"09{n:03d}" for n in range(1, 16, 2)}
CT_PLANNING = {f"091{n}0" for n in range(1, 10)}
LIST_LIMIT = 25
HOSPITAL_SOURCES = {
    "CMS_HCRIS_PUF": "all",
    "CMS_POS": "hospital_category_01",
    "main-cmi-ipps": "cmi_tables",
    "CMS_IPPS": "all",
    "CMS_MEDICARE_PROVIDER": "all",
    "HHS": "all",
    "ENROLL": "all",
    "ONC_PI": "all",
}


def county_category(code: str) -> str:
    """Which part of the geography universe a five-digit code belongs to."""
    state, county = code[:2], code[2:]
    if code == "00000":
        return "national_total"
    if county == "000":
        return "state_total"
    if county in {"999", "990"} or county.startswith("99"):
        return "unknown_or_unassigned_county"
    if code in CT_PLANNING:
        return "connecticut_planning_region"
    if code in CT_LEGACY:
        return "connecticut_legacy_county"
    if state in STATES:
        return "state_county"
    if state == "72":
        return "puerto_rico_municipio"
    if state in TERRITORIES:
        return "territory_area"
    return "unrecognized_state_code"


def reference_for(period: str, adjacency: dict[str, list[str]]) -> tuple[str, set[str]] | None:
    """The Census county adjacency vintage used as the reference universe for a period."""
    if not adjacency or not period[:4].isdigit():
        return None
    year = int(period[:4])
    vintage = "2010" if year < 2022 or "2023" not in adjacency else "2023"
    if vintage not in adjacency:
        return None
    return vintage, {c for c in adjacency[vintage] if c[:2] in STATES}


def county_universes(keysets: dict[str, Any]) -> dict[str, Any]:
    """Per source and period: category counts, Connecticut coding and differences from the reference universe."""
    adjacency = keysets.get("ADJ", {}).get("county", {}).get("all", {})
    result: dict[str, Any] = {}
    for source, kinds in sorted(keysets.items()):
        for subset, periods in sorted(kinds.get("county", {}).items()):
            for period, codes in sorted(periods.items()):
                present = set(codes)
                categories = Counter(county_category(code) for code in present)
                entry: dict[str, Any] = {
                    "distinct": len(present),
                    "categories": dict(sorted(categories.items())),
                    "connecticut": "both"
                    if present & CT_LEGACY and present & CT_PLANNING
                    else "legacy"
                    if present & CT_LEGACY
                    else "planning"
                    if present & CT_PLANNING
                    else "none",
                }
                reference = reference_for(period, adjacency)
                if reference is not None:
                    vintage, universe = reference
                    states = {
                        c
                        for c in present
                        if c[:2] in STATES and county_category(c) in {"state_county", "connecticut_legacy_county", "connecticut_planning_region"}
                    }
                    missing, extra = sorted(universe - states), sorted(states - universe)
                    entry["reference"] = {
                        "adjacency_vintage": vintage,
                        "reference_counties_50_states_dc": len(universe),
                        "missing": len(missing),
                        "extra": len(extra),
                        "missing_codes": missing[:LIST_LIMIT],
                        "extra_codes": extra[:LIST_LIMIT],
                    }
                result[f"{source}|{subset}|{period}"] = entry
    return result


def ccn_bucket(ccn: str) -> str:
    """A coarse, publisher-neutral bucket of a CCN's last four characters, for describing unmatched facilities."""
    tail = ccn[2:]
    if not tail.isdigit():
        return "letter_in_positions_3_to_6"
    return f"{tail[0]}xxx"


def nearest(periods: dict[str, list[str]], year: str) -> str | None:
    """The same year when present, else the closest earlier year, else none."""
    if year in periods:
        return year
    earlier = [p for p in periods if p[:4].isdigit() and p[:4] < year]
    return max(earlier) if earlier else None


def calendar_windows(periods: dict[str, list[str]]) -> dict[str, list[str]]:
    """HAI measurement windows that are exact calendar years, keyed by that year."""
    return {window[:4]: codes for window, codes in periods.items() if window.endswith("-12-31") and window[5:10] == "01-01" and window[:4] == window[11:15]}


def hospital_coverage(keysets: dict[str, Any]) -> dict[str, Any]:
    """Share of HAI facilities with a numeric SIR whose CCN appears in each hospital source for the same (or nearest earlier) year."""
    hai = calendar_windows(keysets.get(HAI, {}).get("hospital", {}).get("numeric_sir", {}))
    result: dict[str, Any] = {}
    for year, facilities in sorted(hai.items()):
        wanted = set(facilities)
        row: dict[str, Any] = {"hai_facilities_with_numeric_sir": len(wanted)}
        for source, subset in sorted(HOSPITAL_SOURCES.items()):
            periods = keysets.get(source, {}).get("hospital", {}).get(subset, {})
            if source in {"ENROLL", "ONC_PI"}:
                pool = {c for codes in periods.values() for c in codes}
                used = "union_of_all_periods"
            else:
                chosen = nearest(periods, year)
                if chosen is None:
                    row[source] = {"status": "no_period_at_or_before"}
                    continue
                pool, used = set(periods[chosen]), chosen
            unmatched = wanted - pool
            row[source] = {
                "period_used": used,
                "matched": len(wanted) - len(unmatched),
                "share": round((len(wanted) - len(unmatched)) / len(wanted), 4) if wanted else None,
                "unmatched_by_ccn_bucket": dict(sorted(Counter(ccn_bucket(c) for c in unmatched).items())),
            }
        result[year] = row
    return result


def hud_period(periods: dict[str, list[str]], year: str) -> str | None:
    """The latest HUD quarter within a year."""
    within = sorted(p for p in periods if p.startswith(f"{year}-"))
    return within[-1] if within else None


def zip_routes(keysets: dict[str, Any]) -> dict[str, Any]:
    """HAI hospital ZIPs in the HUD ZIP-to-county file of the same year, county multiplicity and agreement with POS counties."""
    hud_pairs = keysets.get("HUD", {}).get("zip|county", {}).get("all", {})
    hai_pairs = calendar_windows(keysets.get(HAI, {}).get("hospital|zip", {}).get("listed", {}))
    pos_pairs = keysets.get("CMS_POS", {}).get("hospital|county", {}).get("hospital_category_01", {})
    result: dict[str, Any] = {}
    for year, pairs in sorted(hai_pairs.items()):
        quarter = hud_period(hud_pairs, year)
        entry: dict[str, Any] = {"hai_facility_zip_pairs": len(pairs), "hud_quarter": quarter}
        if quarter is None:
            result[year] = {**entry, "status": "no_hud_quarter_in_year"}
            continue
        counties: dict[str, set[str]] = {}
        for pair in hud_pairs[quarter]:
            zip_code, county = pair.split("|")
            counties.setdefault(zip_code, set()).add(county)
        multiplicity: Counter[str] = Counter()
        single: dict[str, str] = {}
        for pair in pairs:
            ccn, zip_code = pair.split("|")
            found = counties.get(zip_code)
            multiplicity["not_in_hud" if not found else "1" if len(found) == 1 else "2" if len(found) == 2 else "3_or_more"] += 1
            if found and len(found) == 1:
                single[ccn] = next(iter(found))
        entry["zip_county_multiplicity"] = dict(sorted(multiplicity.items()))
        pos_year = nearest(pos_pairs, year)
        if pos_year is not None:
            pos: dict[str, set[str]] = {}
            for pair in pos_pairs[pos_year]:
                ccn, county = pair.split("|")
                pos.setdefault(ccn, set()).add(county)
            agreement = Counter("ccn_not_in_pos" if ccn not in pos else "agree" if county in pos[ccn] else "disagree" for ccn, county in single.items())
            entry["single_county_zip_vs_pos_county"] = {"pos_period": pos_year, **dict(sorted(agreement.items()))}
        result[year] = entry
    return result


def time_coverage(keysets: dict[str, Any]) -> dict[str, Any]:
    """Periods present for every source, key kind and subset."""
    return {
        f"{source}|{kind}|{subset}": sorted(periods)
        for source, kinds in sorted(keysets.items())
        for kind, subsets in sorted(kinds.items())
        if "|" not in kind
        for subset, periods in sorted(subsets.items())
    }


def main() -> int:
    """Run the comparison from its real CLI."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--keysets", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    document = json.loads(args.keysets.read_text())
    keysets = document["keysets"]
    report = {
        "version": 1,
        "keysets_sha256": digest(args.keysets),
        "extraction_code_sha256": document["code_sha256"],
        "code_sha256": digest(Path(__file__)),
        "county_universes": county_universes(keysets),
        "hospital_coverage": hospital_coverage(keysets),
        "zip_routes": zip_routes(keysets),
        "time_coverage": time_coverage(keysets),
        "model_eligible": False,
        "limits": [
            "Set comparisons of public identifiers within a stated period; not joins and not entity matching.",
            "Reference universe: Census county adjacency 2010 for periods before 2022, 2023 from 2022; 50 states and DC only.",
            "HAI periods are the calendar-year measurement windows (2018, 2019, 2021 to 2024).",
            "Each hospital source uses the same year or the nearest earlier year; ENROLL and ONC_PI use all periods.",
            "HUD uses the latest quarter within the HAI window end year.",
        ],
    }
    write_json(args.output, report)
    sys.stdout.write(json.dumps({"county_entries": len(report["county_universes"]), "hai_years": sorted(report["hospital_coverage"])}) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
