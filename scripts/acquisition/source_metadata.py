"""Derive dataset placements from tracked configuration and extract public HTML attribution offline."""

from __future__ import annotations

import argparse
import ast
import csv
import json
import re
import sys
from collections import defaultdict
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

from scripts.acquisition.source_registry import REGISTRY_PATH, RegistryError, index_records, read_json, require

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY = REGISTRY_PATH
LAYOUT = REPO_ROOT / "config/acquisition/collection_layout.json"
ADDITIONS = REGISTRY.with_name("registry_additions.json")
BRONZE_TABLES = REPO_ROOT / "config/lakehouse/bronze_tables.json"
RETIRED_OBJECTS = REPO_ROOT / "config/lakehouse/retired_objects.json"
DATASET_LAYOUT = REPO_ROOT / "scripts/acquisition/dataset_layout.py"
RULE_ORDER = (
    "id_equal_source_folder",
    "cms_token_in_landing_url",
    "cms_token_in_registry_record",
    "cms_alias_in_dataset_layout",
    "single_source_collection",
    "retired_object_key",
)
ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9_]{0,119}")


def source_folder(source_id: str) -> str:
    """Return the dataset folder name derived from a source ID (the same rule as dataset_layout.source_folder)."""
    return source_id.replace("-", "_").lower()


def strings_of(value: Any) -> list[str]:
    """Return every string inside a nested JSON value."""
    if isinstance(value, dict):
        return [text for item in value.values() for text in strings_of(item)]
    if isinstance(value, list):
        return [text for item in value for text in strings_of(item)]
    return [value] if isinstance(value, str) else []


def cms_aliases() -> dict[str, str]:
    """Read CMS_ALIASES (publisher dataset ID -> dataset folder) from dataset_layout.py without importing it."""
    for node in ast.parse(DATASET_LAYOUT.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "CMS_ALIASES" for t in node.targets):
            return dict(ast.literal_eval(node.value))
    raise SystemExit("CMS_ALIASES not found in dataset_layout.py")


def candidate_placements(
    ids: list[str], registry: dict[str, Any]
) -> tuple[dict[str, dict[str, list[tuple[str, str]]]], dict[str, set[str]], dict[str, set[str]]]:
    """Return {dataset_id: {rule: [(source_id, detail)]}}, the bronze collections per dataset_id and the collection per source."""
    sources = {source["source_id"]: source for source in registry["sources"]}
    layout = json.loads(LAYOUT.read_text(encoding="utf-8"))["groups"]
    collection_of = {source: f"{group['publisher']}/{group['collection']}" for group in layout for source in group["source_ids"]}
    sources_in = defaultdict(list)
    for source, collection in collection_of.items():
        sources_in[collection].append(source)
    tables = json.loads(BRONZE_TABLES.read_text(encoding="utf-8"))["tables"]
    table_collections: dict[str, set[str]] = defaultdict(set)
    table_names: dict[str, set[str]] = defaultdict(set)
    for table in tables:
        for dataset_id in table.get("dataset_ids", []):
            table_collections[dataset_id].add(table["collection"])
            table_names[dataset_id].add(table["table"])
    retired: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for item in json.loads(RETIRED_OBJECTS.read_text(encoding="utf-8"))["objects"]:
        match = re.match(r"[a-z0-9_]+/[a-z0-9_]+/datasets/([a-z0-9_]+)/capture_id=(.+?)__", item["key"])
        if match:
            retired[match.group(1)].add((match.group(2), item["key"].split("/datasets/")[0]))
    aliases = cms_aliases()
    found: dict[str, dict[str, list[tuple[str, str]]]] = {}
    for dataset_id in ids:
        rules: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for source in sources:
            if source_folder(source) == dataset_id:
                rules["id_equal_source_folder"].append((source, f"the dataset folder rule applied to source ID {source} gives {dataset_id}"))
        token_text = re.sub(r"^cms_(?:legacy_)?(?:pdc_s3_hos_data_)?", "", dataset_id)
        token = token_text.replace("_", "-") if dataset_id.startswith("cms_") and re.fullmatch(r"[a-z0-9]{4}_[a-z0-9]{4}", token_text) else None
        if dataset_id in aliases.values():
            token = next(native for native, folder in aliases.items() if folder == dataset_id)
            for source, record in sources.items():
                if token in record["official_landing_url"]:
                    rules["cms_alias_in_dataset_layout"].append(
                        (source, f"dataset_layout.CMS_ALIASES maps {token} to {dataset_id}; the landing URL of {source} contains {token}")
                    )
        if token and dataset_id not in aliases.values():
            for source, record in sources.items():
                if token in record["official_landing_url"]:
                    rules["cms_token_in_landing_url"].append((source, f"the landing URL of {source} contains the publisher dataset ID {token}"))
            if not rules.get("cms_token_in_landing_url"):
                for source, record in sources.items():
                    if any(token in text for text in strings_of(record)):
                        rules["cms_token_in_registry_record"].append((source, f"the registry record of {source} names a member file that starts with {token}"))
        collections = table_collections.get(dataset_id, set())
        if len(collections) == 1:
            collection = next(iter(collections))
            owners = sources_in.get(collection, [])
            if len(owners) == 1:
                tables_text = ", ".join(sorted(table_names[dataset_id]))
                rules["single_source_collection"].append(
                    (owners[0], f"bronze table {tables_text} selects it in collection {collection}, which collection_layout gives to {owners[0]} only")
                )
        for source_id, prefix in sorted(retired.get(dataset_id, set())):
            if source_id in sources:
                rules["retired_object_key"].append((source_id, f"retired_objects.json holds a key {prefix}/datasets/{dataset_id}/capture_id={source_id}__..."))
        found[dataset_id] = dict(rules)
    return found, {key: set(value) for key, value in table_collections.items()}, {source: {collection} for source, collection in collection_of.items()}


def decide(
    found: dict[str, dict[str, list[tuple[str, str]]]], table_collections: dict[str, set[str]], source_collection: dict[str, set[str]]
) -> tuple[dict[str, tuple[str, str, str]], dict[str, str]]:
    """Return placements {dataset_id: (source_id, rule, detail)} and reasons for every dataset_id left unplaced."""
    placed: dict[str, tuple[str, str, str]] = {}
    unplaced: dict[str, str] = {}
    for dataset_id, rules in found.items():
        candidates = {source for hits in rules.values() for source, _ in hits}
        if not candidates:
            selected_in = sorted(table_collections.get(dataset_id, set()))
            owners = sorted(source for source, collections in source_collection.items() if collections & set(selected_in))
            unplaced[dataset_id] = (
                f"no rule places it; bronze_tables.json selects it in {', '.join(selected_in)}, "
                f"which collection_layout gives to {len(owners)} sources: {', '.join(owners)}"
                if selected_in
                else "no rule places it and no committed configuration names it"
            )
            continue
        if len(candidates) > 1:
            unplaced[dataset_id] = "rules disagree: " + "; ".join(f"{rule} gives {sorted({s for s, _ in hits})}" for rule, hits in sorted(rules.items()))
            continue
        source = next(iter(candidates))
        collections = table_collections.get(dataset_id, set())
        if collections and not collections <= source_collection[source]:
            unplaced[dataset_id] = f"collection mismatch: placed in {source} ({sorted(source_collection[source])}) but selected in {sorted(collections)}"
            continue
        first = next(rule for rule in RULE_ORDER if rule in rules)
        agreeing = [rule for rule in RULE_ORDER if rule in rules and rule != first]
        detail = rules[first][0][1] + (f"; also by {', '.join(agreeing)}" if agreeing else "")
        placed[dataset_id] = (source, first, detail)
    return placed, unplaced


def derive(ids: list[str], registry: dict[str, Any]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Return deterministic placements and unplaced evidence for an explicit dataset inventory."""
    require(all(isinstance(value, str) and ID_PATTERN.fullmatch(value) for value in ids), "Invalid placement dataset_id.")
    require(len(ids) == len(set(ids)), "Duplicate placement dataset_id.")
    placed, unplaced = decide(*candidate_placements(sorted(ids), registry))
    placements = [{"dataset_id": value, "source_id": placed[value][0], "rule": placed[value][1], "detail": placed[value][2]} for value in sorted(placed)]
    unresolved = [{"dataset_id": value, "detail": unplaced[value]} for value in sorted(unplaced)]
    return placements, unresolved


def validate_placements(additions: dict[str, Any], registry: dict[str, Any]) -> tuple[int, int]:
    """Re-derive every stored placement and unplaced reason using only delivered configuration."""
    present = ("dataset_id_placements" in additions, "unplaced_dataset_ids" in additions)
    if present == (False, False):
        require("source_metadata" not in additions, "Source metadata needs tracked placement sections.")
        return 0, 0
    require(all(present), "Placement and unplaced sections must exist together.")
    placed = index_records(additions["dataset_id_placements"], "dataset_id", "placement")
    unplaced = index_records(additions["unplaced_dataset_ids"], "dataset_id", "unplaced dataset")
    require(not placed.keys() & unplaced.keys(), "A dataset_id cannot be both placed and unplaced.")
    for record in placed.values():
        require(set(record) == {"dataset_id", "source_id", "rule", "detail"}, "Malformed stored placement.")
    for record in unplaced.values():
        require(set(record) == {"dataset_id", "detail"}, "Malformed unplaced evidence.")
    expected, unresolved = derive([*placed, *unplaced], registry)
    require(sorted(placed.values(), key=lambda item: item["dataset_id"]) == expected, "Stored placements differ from tracked derivation.")
    require(sorted(unplaced.values(), key=lambda item: item["dataset_id"]) == unresolved, "Unplaced evidence differs from tracked derivation.")
    for entry in additions.get("source_metadata", []):
        records = [record for record in expected if record["source_id"] == entry["source_id"]]
        evidence = [{key: value for key, value in record.items() if key != "source_id"} for record in records]
        require(entry["dataset_ids"] == [record["dataset_id"] for record in records], "Source dataset_ids differ from tracked derivation.")
        catalog_evidence = [{key: value for key, value in record.items() if key != "acquired_via"} for record in entry["dataset_ids_evidence"]]
        require(catalog_evidence == evidence, "Source dataset evidence differs from tracked derivation.")
    return len(placed), len(unplaced)


class PageText(HTMLParser):
    """Preserve descendant text in headings, footer elements and visible blocks."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.pairs: list[tuple[str, str]] = []
        self.stack: list[tuple[str, list[str]]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Keep meta attribution and start non-void text containers."""
        values = dict(attrs)
        if tag == "meta" and values.get("content"):
            name = (values.get("name") or values.get("property") or "").lower()
            self.pairs.append((f"meta:{name}", str(values["content"]).strip()))
        if tag not in {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}:
            self.stack.append((tag, []))
        elif tag in {"br", "hr"}:
            self.handle_data(" ")

    def handle_endtag(self, tag: str) -> None:
        """Close through the matching element without dropping nested text."""
        matching = next((i for i in range(len(self.stack) - 1, -1, -1) if self.stack[i][0] == tag), None)
        if matching is None:
            return
        for name, parts in self.stack[matching:]:
            text = re.sub(r"\s+", " ", "".join(parts)).strip()
            if text:
                field = "title" if name == "title" else "heading" if name in {"h1", "h2", "h3"} else "footer" if name == "footer" else "visible"
                self.pairs.append((field, text))
        del self.stack[matching:]

    def handle_data(self, data: str) -> None:
        """Append descendant text to each ancestor's independent buffer."""
        for _, parts in self.stack:
            parts.append(data)


def organization_names(value: Any) -> list[str]:
    """Find structured Organization names within JSON-LD objects, arrays and graphs."""
    result: list[str] = []
    if isinstance(value, dict):
        types = value.get("@type", [])
        types = [types] if isinstance(types, str) else types
        if isinstance(types, list) and any(str(kind).rsplit("/", 1)[-1] == "Organization" for kind in types) and isinstance(value.get("name"), str):
            result.append(value["name"])
        for child in value.values():
            result.extend(organization_names(child))
    elif isinstance(value, list):
        for child in value:
            result.extend(organization_names(child))
    return result


def html_attribution(text: str) -> list[tuple[str, str]]:
    """Extract visible and structured names without treating code or style text as visible."""
    pairs: list[tuple[str, str]] = []
    for match in re.finditer(r"<script[^>]*ld\+json[^>]*>(.*?)</script>", text, re.S | re.I):
        try:
            document = json.loads(match.group(1))
        except ValueError:
            pairs.append(("unsupported_jsonld", "Invalid JSON-LD"))
        else:
            pairs.extend(("jsonld:Organization", name) for name in organization_names(document))
    parser = PageText()
    parser.feed(re.sub(r"<script[^>]*>.*?</script>|<style[^>]*>.*?</style>", " ", text, flags=re.S | re.I))
    parser.handle_endtag("html")
    return pairs + parser.pairs


def main() -> None:
    """Check delivered placements or derive them from an explicitly supplied dataset CSV."""
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--dataset-ids", type=Path, help="Explicit CSV input for a derivation run; never needed by --check.")
    parser.add_argument("--additions", type=Path, default=ADDITIONS)
    parser.add_argument("--bronze-lineage", type=Path, help="Gate placements against the explicit bronze provenance CSV and derive acquisition routes.")
    args = parser.parse_args()
    try:
        from scripts.acquisition.registry_additions import ACQUIRED_VIA_ROUTES, read_bronze_lineage, validate_bronze_lineage, validate_source_metadata

        additions, registry = read_json(args.additions), read_json(REGISTRY)
        lineage = read_bronze_lineage(args.bronze_lineage) if args.bronze_lineage is not None else None
        if args.check:
            validate_source_metadata(additions.get("source_metadata", []), index_records(registry["sources"], "source_id", "source"))
            placed, unplaced = validate_placements(additions, registry)
            if lineage is not None:
                validate_bronze_lineage(additions, lineage)
        else:
            with args.dataset_ids.open(encoding="utf-8", newline="") as handle:
                ids = [row["dataset_id"] for row in csv.DictReader(handle)]
            placements, unresolved = derive(ids, registry)
            additions.update(dataset_id_placements=placements, unplaced_dataset_ids=unresolved)
            for entry in additions.get("source_metadata", []):
                records = [record for record in placements if record["source_id"] == entry["source_id"]]
                previous = {record["dataset_id"]: record.get("acquired_via") for record in entry["dataset_ids_evidence"]}
                entry["dataset_ids"] = [record["dataset_id"] for record in records]
                entry["dataset_ids_evidence"] = [{key: value for key, value in record.items() if key != "source_id"} for record in records]
                for record in entry["dataset_ids_evidence"]:
                    dataset_id = record["dataset_id"]
                    if lineage is not None:
                        require(dataset_id in lineage, f"Placed dataset_id {dataset_id} is absent from bronze lineage.")
                        routes = lineage[dataset_id] - {entry["source_id"]}
                        require(len(routes) <= 1, f"Dataset_id {dataset_id} needs more than one acquired_via; leave it unplaced for review.")
                        via = next(iter(routes)) if routes else None
                    else:
                        via = previous.get(dataset_id)
                    if via is not None:
                        require(
                            via in ACQUIRED_VIA_ROUTES,
                            f"Dataset_id {dataset_id} acquired_via is not a reviewed route; allowed routes: {sorted(ACQUIRED_VIA_ROUTES)}.",
                        )
                        record["acquired_via"] = via
            validate_source_metadata(additions.get("source_metadata", []), index_records(registry["sources"], "source_id", "source"))
            validate_placements(additions, registry)
            if lineage is not None:
                validate_bronze_lineage(additions, lineage)
            args.additions.write_text(json.dumps(additions, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            placed, unplaced = len(placements), len(unresolved)
    except (RegistryError, OSError, KeyError, TypeError, AttributeError) as error:
        parser.error(str(error))
    sys.stdout.write(f"Source placement check passed: {placed} placed; {unplaced} unplaced.\n")
    sys.stdout.write("Bronze lineage gate passed.\n" if lineage is not None else "Bronze lineage gate not run: no lineage CSV supplied.\n")


if __name__ == "__main__":
    main()
