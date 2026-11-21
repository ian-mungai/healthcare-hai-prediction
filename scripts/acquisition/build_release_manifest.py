"""Development step: list the seed files that are assets of the GitHub seed release (name, size, SHA-256 and release address) in the tracked release manifest.

The 34 CMS hospital archives that the owner saved in a browser are too large for Git (579 MB) and public, so they are public assets of one release of this
repository (owner decision "PDC archives as public GitHub release assets", Nov 21 2026). Git tracks only this manifest. After each bootstrap the host downloads
every asset by its address and checks its size and SHA-256 before use (``host_inputs.py``). The manifest takes each size from the unit in the queue and each
digest from the specification of the unit, so no file is needed to build it. ``--check-files`` compares a folder of the files with it.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

from scripts.acquisition.check_baseline_run import ROOT, SEED_ROOT
from scripts.acquisition.host_inputs import RELEASE_KIND, RELEASE_MANIFEST
from scripts.acquisition.redownload_route import RELEASE_SEEDED_MODES
from scripts.acquisition.transport import RELEASE_DOWNLOAD_PREFIX

TAG = "seed-main-hai-pdc-20261120"


def build(queue_bytes: bytes, specs: dict, tag: str = TAG) -> bytes:
    """The release manifest bytes of the units whose seed files come from the release, in seed path order."""
    assets = []
    for unit in json.loads(queue_bytes)["queue"]:
        if unit["disposition"] == "approved_manual" and unit["mode"] in RELEASE_SEEDED_MODES:
            spec = specs["specs"][unit["snapshot_id"]]
            [name] = spec["stored_data_files"]
            assets.append(
                {
                    "name": name,
                    "path": f"manual_units/{unit['snapshot_id']}/{name}",
                    "bytes": unit["bytes"],
                    "sha256": spec["receipt_template"]["expected_sha256"],
                    "url": f"{RELEASE_DOWNLOAD_PREFIX}{tag}/{name}",
                }
            )
    document = {
        "kind": RELEASE_KIND,
        "queue_sha256": hashlib.sha256(queue_bytes).hexdigest(),
        "tag": tag,
        "assets": sorted(assets, key=lambda item: item["path"]),
    }
    return (json.dumps(document, indent=1, sort_keys=True) + "\n").encode()


def file_problems(manifest: dict, folder: Path) -> list[str]:
    """Why the files of a folder are not the release assets: each asset must be there with its size and SHA-256, and no other file may be."""
    found = {path.name: path for path in folder.rglob("*") if path.is_file()}
    problems = [f"{item['name']} is not in the folder" for item in manifest["assets"] if item["name"] not in found]
    for item in manifest["assets"]:
        path = found.get(item["name"])
        if path is not None and (path.stat().st_size != item["bytes"] or hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]):
            problems.append(f"{item['name']} differs from the manifest")
    problems += [f"{name} is in the folder and not in the manifest" for name in sorted(found.keys() - {item["name"] for item in manifest["assets"]})]
    return problems


def main() -> None:
    """Write the manifest of the tracked bundle into the seed folder, or compare a folder of files with the one that is there."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=SEED_ROOT / RELEASE_MANIFEST)
    parser.add_argument("--tag", default=TAG)
    parser.add_argument("--check-files", type=Path, help="compare the files below this folder with the manifest at --output and write nothing")
    args = parser.parse_args()
    if args.check_files is not None:
        found = file_problems(json.loads(args.output.read_text(encoding="utf-8")), args.check_files)
        if found:
            sys.stderr.write("\n".join(found) + "\n")
            raise SystemExit(1)
        sys.stdout.write(json.dumps({"files": "match the release manifest"}) + "\n")
        return
    queue = (args.root / "queue.json").read_bytes()
    specs = json.loads((args.root / "capture_specs.json").read_text(encoding="utf-8"))
    body = build(queue, specs, args.tag)
    args.output.write_bytes(body)
    sys.stdout.write(json.dumps({"assets": len(json.loads(body)["assets"]), "bytes": sum(item["bytes"] for item in json.loads(body)["assets"])}) + "\n")


if __name__ == "__main__":
    main()
