"""Check supplied training, MLflow server and prediction locks without resolving or installing packages."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
TRAINING = {
    "mlflow": "3.17.0",
    "scikit-learn": "1.9.1",
    "xgboost": "3.4.1",
    "shap": "0.52.0",
    "statsmodels": "0.15.0",
    "linearmodels": "7.0",
    "libpysal": "4.15.0",
    "esda": "2.10.0",
    "spreg": "1.9.1",
    "dvc": "3.67.1",
    "dvc-s3": "3.3.0",
}
API = {
    "mlflow": "3.17.0",
    "scikit-learn": "1.9.1",
    "xgboost": "3.4.1",
    "fastapi": "0.142.4",
    "uvicorn": "0.54.0",
    "feast": "0.66.0",
    "redis": "8.1.0",
    "prometheus-client": "0.26.0",
}
SERVER: dict[str, str | None] = {"mlflow": "3.17.0", "boto3": None, "psycopg2-binary": None}
APPROVED: dict[str, dict[str, str | None]] = {"training": dict(TRAINING), "mlflow_server": SERVER, "prediction_api": dict(API)}
RECENT = {"mlflow": "3.17.2", "mlflow-skinny": "3.17.2", "shap": "0.54.1", "linearmodels": "7.1", "libpysal": "4.15.1", "feast": "0.67.0", "fastapi": "0.161.0"}
PIN = re.compile(r"([A-Za-z0-9][A-Za-z0-9_.-]*)==([A-Za-z0-9][A-Za-z0-9.!+_-]*)(.*)")
HASHES = re.compile(r"(?:\s+--hash=sha256:[0-9a-f]{64})+")


class LockError(ValueError):
    """Supplied lock inputs do not establish the required package contract."""


@dataclass(frozen=True)
class Pin:
    version: str
    hashes: frozenset[str]


def _name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _lines(path: Path) -> list[tuple[int, str]]:
    lines = []
    pending = ""
    first = 0
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        value = raw.split("#", 1)[0].strip()
        if not value:
            continue
        if not pending:
            first = number
        pending += " " + value.removesuffix("\\").strip()
        if not value.endswith("\\"):
            lines.append((first, pending.strip()))
            pending = ""
    if pending:
        raise LockError(f"{path.name}:{first}: unfinished line continuation")
    return lines


def _pins(path: Path, *, locked: bool) -> dict[str, Pin]:
    pins: dict[str, Pin] = {}
    binary = False
    for number, line in _lines(path):
        location = f"{path.name}:{number}"
        if locked and line in ("--only-binary=:all:", "--only-binary :all:"):
            binary = True
            continue
        match = PIN.fullmatch(line)
        if not match:
            raise LockError(f"{location}: exact pin required; includes, URLs and installer options are not allowed")
        package, version, tail = _name(match[1]), match[2], match[3]
        if package in pins:
            raise LockError(f"{location}: duplicate package {package}")
        if locked:
            if not HASHES.fullmatch(tail):
                raise LockError(f"{location}: sha256 hashes are required for {package}")
        elif tail:
            raise LockError(f"{location}: direct requirement must be an exact pin without options")
        hashes = frozenset(re.findall(r"--hash=sha256:([0-9a-f]{64})", tail))
        pins[package] = Pin(version, hashes)
    if not pins:
        raise LockError(f"{path.name}: no pinned packages")
    if locked and not binary:
        raise LockError(f"{path.name}: only-binary=:all: is required")
    return pins


def _inventory(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
        schema = json.loads((ROOT / "data_contracts/training/lock_evidence.schema.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as read_error:
        raise LockError(f"wheel inventory: cannot read evidence ({type(read_error).__name__})") from read_error
    errors = list(Draft202012Validator(schema).iter_errors(document))
    if errors:
        error = errors[0]
        field = ".".join(str(part) for part in error.absolute_path)
        category = "release history" if "release_history" in field else "wheel inventory"
        raise LockError(f"{category}: {field or 'root'}: schema {error.validator} failed")
    if not isinstance(document, dict):
        raise LockError("wheel inventory: root: evidence must be a JSON object")
    return document


def _utc(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value)
    if timestamp.utcoffset() != timedelta(0):
        raise LockError("release history: timestamps must use UTC")
    return timestamp.astimezone(UTC)


def _maturity(direct: dict[str, Pin], evidence: dict[str, Any]) -> list[str]:
    problems = []
    cutoff = _utc(evidence["locked_at_utc"]) - timedelta(days=14)
    for package in ("boto3", "psycopg2-binary"):
        eligible = [
            release["version"] for release in evidence["release_history"][package] if not release["yanked"] and _utc(release["uploaded_at_utc"]) <= cutoff
        ]
        if not eligible:
            problems.append(f"mlflow_server: no mature release of {package} in the supplied release history")
            continue
        newest = max(eligible, key=lambda value: tuple(int(part) for part in value.split(".")))
        if package in direct and direct[package].version != newest:
            problems.append(f"mlflow_server: {package} must use the newest mature release from the supplied release history")
    return problems


def _wheel(filename: str) -> tuple[str, str]:
    if Path(filename).name != filename or not filename.endswith(".whl"):
        raise LockError("wheel filename: an unqualified .whl filename is required")
    parts = filename[:-4].split("-")
    if len(parts) not in (5, 6):
        raise LockError("wheel filename: invalid wheel filename")
    python_tag, abi_tag, platform_tag = parts[-3:]
    python_tags, abi_tags = set(python_tag.split(".")), set(abi_tag.split("."))
    compatible = bool(python_tags & {"py3", "py312", "cp312"})
    if "abi3" in abi_tags:
        compatible = compatible or any(re.fullmatch(r"cp3[0-9]+", tag) and 2 <= int(tag[3:]) <= 12 for tag in python_tags)
    if not compatible or not abi_tags.intersection({"none", "abi3", "cp312"}):
        raise LockError("wheel Python: a CPython 3.12 compatible wheel is required")
    platforms = platform_tag.split(".")
    pure = platforms == ["any"] and abi_tags == {"none"}
    arm64 = all(tag == "linux_aarch64" or (tag.startswith("manylinux") and tag.endswith("_aarch64")) for tag in platforms)
    if not pure and not arm64:
        raise LockError("wheel platform: Linux aarch64 or pure Python is required")
    return _name(parts[0]), parts[1]


def _wheel_checks(image: str, pins: dict[str, Pin], evidence: dict[str, Any]) -> list[str]:
    problems, covered = [], set()
    for artifact in evidence["images"][image]:
        try:
            package, version = _wheel(artifact["filename"])
        except LockError as error:
            problems.append(f"{image}: {error}")
            continue
        if package not in pins or pins[package].version != version:
            problems.append(f"{image}: wheel filename does not match a locked package")
        elif artifact["sha256"] not in pins[package].hashes:
            problems.append(f"{image}: wheel hash does not match the lock for {package}")
        else:
            covered.add(package)
    if set(pins) != covered:
        problems.append(f"{image}: wheel inventory needs a matching compatible wheel for every lock package")
    return problems


def check(images: dict[str, tuple[Path, Path]], wheel_inventory: Path) -> dict[str, Any]:
    """Validate supplied pins, shared versions and captured wheel and release evidence."""
    if set(images) != set(APPROVED):
        raise LockError("image inputs: exactly training, mlflow_server and prediction_api are required")
    evidence = _inventory(wheel_inventory)
    direct, locked = {}, {}
    problems = []
    for image, (source, lock) in images.items():
        try:
            direct[image], locked[image] = _pins(source, locked=False), _pins(lock, locked=True)
        except (OSError, LockError) as error:
            problems.append(f"{image}: {error}")
            continue
        approved = APPROVED[image]
        for name, pin in direct[image].items():
            if name not in approved or (approved[name] is not None and pin.version != approved[name]):
                problems.append(f"{image}: direct requirement {name} is not on the approved version list")
        if set(direct[image]) != set(approved):
            problems.append(f"{image}: required packages differ from the approved direct requirements")
        for name, pin in direct[image].items():
            if name not in locked[image] or pin.version != locked[image][name].version:
                problems.append(f"{image}: lock pin differs from the direct requirement for {name}")
        for name, pin in locked[image].items():
            if RECENT.get(name) == pin.version:
                problems.append(f"{image}: recent version is not approved for {name}")
        problems.extend(_wheel_checks(image, locked[image], evidence))
    if "training" in locked:
        for other, packages in (
            ("mlflow_server", ("mlflow", "mlflow-skinny")),
            ("prediction_api", ("mlflow", "mlflow-skinny", "scikit-learn", "xgboost")),
        ):
            if other not in locked:
                continue
            for package in packages:
                expected, found = locked["training"].get(package), locked[other].get(package)
                if expected is None or found is None or expected.version != found.version:
                    problems.append(f"{other}: version parity with training failed for {package}")
    if "mlflow_server" in direct:
        problems.extend(_maturity(direct["mlflow_server"], evidence))
    if problems:
        raise LockError("\n".join(problems))
    return {
        "schema": 1,
        "images": {image: {name: pin.version for name, pin in sorted(pins.items())} for image, pins in sorted(locked.items())},
        "inputs_sha256": {
            image: {"requirements_in": hashlib.sha256(source.read_bytes()).hexdigest(), "lock": hashlib.sha256(lock.read_bytes()).hexdigest()}
            for image, (source, lock) in sorted(images.items())
        },
        "wheel_evidence_sha256": hashlib.sha256(wheel_inventory.read_bytes()).hexdigest(),
        "locked_at_utc": evidence["locked_at_utc"],
        "limits": (
            "Wheel compatibility and server maturity use supplied captured metadata. No package resolution, download, installation or image run is performed."
        ),
    }


def main() -> int:
    """Check explicitly named image inputs and emit a deterministic JSON record."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", required=True)
    parser.add_argument("--image", nargs=3, action="append", metavar=("NAME", "REQUIREMENTS_IN", "LOCK"), required=True)
    parser.add_argument(
        "--wheel-inventory", type=Path, required=True, help="captured wheel hashes and server release history, following lock_evidence.schema.json"
    )
    args = parser.parse_args()
    try:
        images = {name: (Path(source), Path(lock)) for name, source, lock in args.image}
        if len(images) != len(args.image):
            raise LockError("image inputs: duplicate image name")
        record = check(images, args.wheel_inventory)
        sys.stdout.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    except (LockError, OSError, ValueError) as error:
        sys.stderr.write(f"{error}\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
