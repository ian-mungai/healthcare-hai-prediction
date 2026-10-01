"""Enumerate relevant historical files from preserved official publisher links."""

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import urldefrag, urlsplit

from scripts.acquisition.discover_history import candidate, index
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import load_registry


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", type=Path, required=True)
    args = parser.parse_args()
    root = args.state_root
    sources = {s["source_id"]: s for s in load_registry()["sources"]}
    candidates: dict[str, dict] = {}

    def add(source: str, item: dict, evidence: dict, role: str = "data") -> None:
        """Retain one supported publisher-listed candidate per unsigned URL."""
        item = {**item, "url": urldefrag(item["url"])[0]}
        if Path(urlsplit(item["url"]).path).suffix.lower() not in {".csv", ".txt", ".zip", ".xls", ".xlsx", ".pdf", ".docx"}:
            return
        record = candidate(source, item, evidence, 128 * 1024**2)
        record["role"] = role
        candidates.setdefault(item["url"], record)

    for source in ("RUCC", "RUCA", "ADJ", "ONC_PI", "CHIA", "MARY"):
        links, evidence = index(sources[source]["official_landing_url"], root)
        for item in links:
            text = (item["url"] + " " + item["label"]).lower()
            select = (
                (source in {"RUCC", "RUCA"} and "/media/" in item["url"])
                or source == "ADJ"
                and "county_adjacency" in text
                or source == "ONC_PI"
                and "hospital-promoting-interoperability" in text
                or source == "CHIA"
                and "workforce-survey" in text
                or source == "MARY"
                and "/reports/" in text
            )
            if select:
                role = "reference" if any(w in text for w in ("technical", "guide", "manual", "classification-examples", "collection-tool")) else "data"
                add(source, item, evidence, role)
            if source == "ADJ" and re.search(r"county-adjacency\.\d{4}\.html", item["url"]):
                nested, subevidence = index(urldefrag(item["url"])[0], root)
                for child in nested:
                    if "county_adjacency" in child["url"]:
                        add(source, child, subevidence)

    links, evidence = index(sources["main-cmi-ipps"]["official_landing_url"], root)
    pages = [item for item in links if re.search(r"Files for FY|Case Mix Index", item["label"], re.I)]
    pages.append({"url": sources["CMS_OCCMIX"]["official_landing_url"], "label": "Wage index"})
    for page in {p["url"]: p for p in pages}.values():
        children, subevidence = index(page["url"], root)
        for item in children:
            text = (item["url"] + " " + item["label"]).lower().replace("-", " ")
            selected_source = (
                "main-cmi-ipps"
                if any(word in text for word in ("case mix", "cmi", "alternative"))
                else "CMS_IPPS"
                if "impact" in text
                else "CMS_OCCMIX"
                if any(word in text for word in ("occupational", "occmix", "wage index puf", "wageindexpuf"))
                else None
            )
            if selected_source:
                add(selected_source, item, subevidence)

    report_pages = {
        "TX": [
            sources["TX"]["official_landing_url"],
            "https://www.dshs.texas.gov/texas-center-nursing-workforce-studies/employer-nurse-staffing-studies/employer-nurse-staffing-studies",
        ],
        "NY": ["https://www.health.ny.gov/statistics/facilities/hospital/hospital_acquired_infections/"],
        "VT": ["https://www.healthvermont.gov/systems/hospitals-health-systems/hospital-report-cards"],
        "AHRF": ["https://data.hrsa.gov/data/download?AHRF=&data=AHRF"],
    }
    for source, urls in report_pages.items():
        for url in urls:
            links, evidence = index(url, root)
            for item in links:
                text = item["url"].lower()
                selected = (
                    (source == "TX" and "/hnss/" in text)
                    or source == "NY"
                    and "hospital_acquired" in text
                    or source == "VT"
                    and ("hrc-ns-" in text or "nurse-staffing" in text)
                    or source == "AHRF"
                    and "/ahrf/" in text
                    and "sas" not in text
                    and "sn_" not in text
                )
                if selected:
                    role = "reference" if any(word in text for word in ("user", "tech", "guide", "template", "manual")) else "data"
                    add(source, item, evidence, role)
    write_once(root / "candidates.json", encoded_json({"candidates": list(candidates.values())}))
    sys.stdout.write(str(f"Publisher-listed historical candidates: {len(candidates)}") + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
