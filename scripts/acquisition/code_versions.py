"""Read exact reviewed fingerprints without API-shaped JSON key/value pairs."""

import re
from pathlib import Path, PurePosixPath

from scripts.acquisition.source_registry import read_json, require


def read_code_versions(path: Path) -> dict:
    """Normalize legacy maps and typed entries; never add a reviewed fingerprint."""
    document = read_json(path)
    require(isinstance(document.get("versions"), list), "Missing reviewed code versions")
    for version in document["versions"]:
        records = version.get("code_sha256")
        require(isinstance(records, (dict, list)), "Malformed reviewed fingerprints")
        if isinstance(records, dict):
            pairs = list(records.items())
        else:
            require(all(isinstance(row, dict) and set(row) == {"file", "sha256"} for row in records), "Malformed typed fingerprint")
            pairs = [(row["file"], row["sha256"]) for row in records]
        normalized = {}
        for name, digest in pairs:
            require(
                isinstance(name, str) and name.startswith("scripts/") and ".." not in PurePosixPath(name).parts and not PurePosixPath(name).is_absolute(),
                "Invalid fingerprint path",
            )
            require(isinstance(digest, str) and re.fullmatch(r"[a-f0-9]{64}", digest) is not None, "Invalid fingerprint digest")
            require(name not in normalized, "Duplicate fingerprint path")
            normalized[name] = digest
        require(bool(normalized), "Empty reviewed fingerprints")
        version["code_sha256"] = normalized
    return document
