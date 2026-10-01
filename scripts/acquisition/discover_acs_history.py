"""Locate publisher-listed five-year table files for the already reviewed ACS fields."""

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import urlsplit

from scripts.acquisition.discover_history import candidate, index
from scripts.acquisition.s3_store import encoded_json, write_once
from scripts.acquisition.source_registry import load_registry


def main() -> None:
    """Run the command-line workflow with the supplied arguments and report its outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-root", type=Path, required=True)
    args = parser.parse_args()
    fields = " ".join(m["preserved_controls"].get("current_exact_field") or "" for m in load_registry()["measure_controls"])
    tables = set(re.findall(r"\b(?:B\d{5}[A-I]?|C\d{5})(?=[_\s;,.])", fields))
    root = args.state_root
    base = "https://www2.census.gov/programs-surveys/acs/summary_file/"
    years, _ = index(base, root)
    candidates, layouts = {}, []
    for year in years:
        if not re.fullmatch(r"\d{4}/", year["url"].removeprefix(base)):
            continue
        links, evidence = index(year["url"], root)
        layouts.append({"release_directory": year["url"], "children": links})
        selected = [link for link in links if link["label"] in {"prototype/", "table-based-SF/"}]
        queue = [(link["url"], 0) for link in selected]
        while queue:
            url, depth = queue.pop(0)
            children, evidence = index(url, root)
            for item in children:
                name = Path(urlsplit(item["url"]).path).name
                if depth < 3 and item["label"] in {"data/", "5YRData/", "documentation/"}:
                    queue.append((item["url"], depth + 1))
                elif "5YRData/" in url and name.lower().endswith(".dat") and any(re.search(r"[-_]" + t.lower() + r"\.dat$", name.lower()) for t in tables):
                    candidates[item["url"]] = candidate("ACS", item, evidence, 128 * 1024**2)
                elif any(word in name.lower() for word in ("shell", "geos", "readme", "user", "technical")) and Path(name).suffix.lower() in {
                    ".csv",
                    ".txt",
                    ".pdf",
                    ".xlsx",
                }:
                    row = candidate("ACS", item, evidence, 128 * 1024**2)
                    row["role"] = "reference"
                    candidates[item["url"]] = row
    write_once(
        root / "candidates.json",
        encoded_json(
            {
                "candidates": list(candidates.values()),
                "reviewed_table_codes": sorted(tables),
                "layout_inventory": layouts,
                "subject_and_profile_tables_remain_separate": True,
            }
        ),
    )
    sys.stdout.write(str(f"ACS historical table and reference files: {len(candidates)}") + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
