"""One-time development step: the seeded copy of the 2018 AHRQ Compendium workbook, without the workstation path element (owner, Nov 19 2026).

AHRQ's ``chsp-hospital-linkage-2018.xlsx`` holds an absolute path of the authoring computer, as the element ``x15ac:absPath`` inside an
``mc:AlternateContent`` block of ``xl/workbook.xml``. The path has a user directory, which is likely a person's name, and the repository is public. The seed
holds a COPY of the workbook with exactly that block removed. Every other member keeps its name, its content and its place in the archive. The step refuses an
input other than the recorded original, a workbook with no such block or with more than one, a block that holds more than the path element, a path element
outside a block, and a path element in another member. It never prints or returns the path.

The plan entry and the unit bind the copy by its own SHA-256 and size. The specification of the unit records the original's SHA-256 and size and the change
(``COPIES``), and the manifest takes the digest of the copy from the plan. Run it from the repository root::

    python -m scripts.acquisition.ahrq_seed_copy --original <the original workbook> --output <the copy>

It writes the copy and updates the plan entry and the plan lock. A second run with the same original gives the same bytes and changes nothing. Then run
``add_ahrq_units`` (it rebuilds the units), ``build_seed_manifest`` and ``check_baseline_run --write-lock``, and move the copy to its seed folder.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import io
import json
import re
import sys
import zipfile
from pathlib import Path

from defusedxml import ElementTree

from scripts.acquisition.ahrq_compendium_contract import PLAN_PATH, file_id
from scripts.acquisition.s3_store import encoded_json
from scripts.acquisition.source_registry import canonical_hash

NAME = "chsp-hospital-linkage-2018.xlsx"
WORKBOOK = "xl/workbook.xml"
CHANGE = "workstation path element removed from xl/workbook.xml"
# The original as the publisher served it and the owner saved it (the plan held these before the copy), and the change that makes the seeded copy.
COPIES: dict[str, dict[str, str | int]] = {
    NAME: {"original_sha256": "0c4a56df0c25168a23cef0e62af2eeec335176d808f4f235ff10733073c89880", "original_bytes": 654428, "change": CHANGE},
}
BLOCKS = re.compile(rb"<mc:AlternateContent\b.*?</mc:AlternateContent>", re.DOTALL)
STRICT = re.compile(
    rb'^<mc:AlternateContent(?:\s+xmlns:mc="[^"]*")?\s*><mc:Choice\s+Requires="x15"\s*>'
    rb'<x15ac:absPath(?:\s+(?:url|xmlns:x15ac)="[^"]*"){1,2}\s*/></mc:Choice></mc:AlternateContent>$'
)
ELEMENT = b"absPath"


def remove_block(workbook: bytes) -> bytes:
    """The workbook part without its one path block. Refuses anything but exactly one block that holds only the path element."""
    total = workbook.count(ELEMENT)
    if total == 0:
        raise ValueError(f"{WORKBOOK} has no workstation path element")
    holding = [block for block in BLOCKS.findall(workbook) if ELEMENT in block]
    if len(holding) > 1:
        raise ValueError(f"{WORKBOOK} has more than one block with a workstation path element")
    if not holding or sum(block.count(ELEMENT) for block in holding) != total:
        raise ValueError(f"{WORKBOOK} has a workstation path element outside an AlternateContent block")
    if not STRICT.match(holding[0]):
        raise ValueError(f"the AlternateContent block of {WORKBOOK} holds more than the path element")
    return workbook.replace(holding[0], b"", 1)


def clean(original: bytes, expected: tuple[str, int] | None = None) -> bytes:
    """The seeded copy of ``original``: the same archive without the path block. ``expected`` is the original's SHA-256 and size, or None to skip that check."""
    if expected is not None and (hashlib.sha256(original).hexdigest(), len(original)) != expected:
        raise ValueError("the input is not the recorded original")
    try:
        source = zipfile.ZipFile(io.BytesIO(original))
    except zipfile.BadZipFile:
        raise ValueError("the input is not a workbook package") from None
    with source:
        infos = source.infolist()
        if WORKBOOK not in {info.filename for info in infos}:
            raise ValueError("the input is not a workbook package")
        bodies = {info.filename: source.read(info) for info in infos}
    if any(ELEMENT in body for name, body in bodies.items() if name != WORKBOOK):
        raise ValueError("a workstation path element is in another member than the workbook part")
    bodies[WORKBOOK] = remove_block(bodies[WORKBOOK])
    ElementTree.fromstring(bodies[WORKBOOK])
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as target:
        for info in infos:
            member = zipfile.ZipInfo(info.filename, info.date_time)
            member.compress_type, member.external_attr, member.create_system = info.compress_type, info.external_attr, info.create_system
            target.writestr(member, bodies[info.filename], compresslevel=9)
    result = buffer.getvalue()
    with zipfile.ZipFile(io.BytesIO(result)) as check:
        if [i.filename for i in check.infolist()] != [i.filename for i in infos] or check.testzip() is not None:
            raise ValueError("the copy does not keep the member names and order")
        if any(check.read(name) != bodies[name] for name in bodies) or ELEMENT in result:
            raise ValueError("the copy differs from the intended content")
    return result


def update_plan(plan: dict, body: bytes) -> dict:
    """The plan with the entry of the seeded copy bound to the copy's SHA-256 and size and its file ID recomputed. The input plan is not changed."""
    updated = copy.deepcopy(plan)
    [entry] = [item for item in updated["files"] if item["file_name"] == NAME]
    entry["sha256"], entry["bytes"] = hashlib.sha256(body).hexdigest(), len(body)
    entry["id"] = file_id(entry)
    return updated


def main() -> None:
    """Write the copy and rebind the plan entry and the plan lock. The output reports the digests and sizes only."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    recorded = COPIES[NAME]
    body = clean(args.original.read_bytes(), (str(recorded["original_sha256"]), int(recorded["original_bytes"])))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(body)
    plan = update_plan(json.loads(PLAN_PATH.read_text(encoding="utf-8")), body)
    PLAN_PATH.write_bytes(encoded_json(plan))
    PLAN_PATH.with_suffix(".lock.json").write_bytes(encoded_json({"plan_sha256": canonical_hash(plan)}))
    sys.stdout.write(json.dumps({"copy_sha256": hashlib.sha256(body).hexdigest(), "copy_bytes": len(body)}) + "\n")


if __name__ == "__main__":
    main()
