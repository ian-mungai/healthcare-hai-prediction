"""Check the pin step in a throwaway Iceberg catalog with synthetic tables; never the project's catalog or S3.

Run ``.venv/bin/python -m scripts.lakehouse.catalog job pins_e2e``; it runs inside the Spark container. Reports stay in
data/e2e/pins/. Failure modes 890 to 892 are in plans/wave1_20261011/gate/pins_failure_modes.md.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.lakehouse import pins

REPORT_ROOT = Path("data/e2e/pins")
NAMESPACE = "pins_check"
# Synthetic tables in the throwaway catalog; the names are constants of this file.
CREATE_SQL = "CREATE TABLE {catalog}.{namespace}.{table} (id INT) USING iceberg"
INSERT_SQL = "INSERT INTO {catalog}.{namespace}.{table} VALUES ({value})"


def refused(action: Callable[[], object], reason: str) -> bool:
    """A call that must raise PinError with its intended reason."""
    try:
        action()
    except pins.PinError as error:
        return reason in str(error)
    return False


class RivalPublishes(Path):
    """A pins path whose existence check passes, after which a rival writer publishes its own pins first [904]."""

    def exists(self, *, follow_symlinks: bool = True) -> bool:
        absent = not super().exists(follow_symlinks=follow_symlinks)
        if absent and self.name == "pins.json":
            self.parent.mkdir(parents=True, exist_ok=True)
            super().write_text('{"first": "1"}\n')
        return not absent


def rival_keeps_its_pin(path: Path) -> bool:
    """A writer that loses the race after its check is refused, the rival's pin stays and no temporary file is left [904]."""
    racing = RivalPublishes(path)
    lost = refused(lambda: pins.write_once(racing, {"first": "2"}), "a run is pinned once")
    leftovers = [item.name for item in path.parent.iterdir() if item.name.endswith(".tmp")]
    return lost and pins.read_pins(path) == {"first": "1"} and not leftovers


def scenarios(spark: Any, root: Path) -> dict[str, bool]:
    """Pin, verify, refusals and a moved table, on two synthetic tables and one empty table."""
    from scripts.lakehouse.session import CATALOG

    sql = spark.sql
    sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    for table in ("first", "second", "empty"):
        sql(CREATE_SQL.format(catalog=CATALOG, namespace=NAMESPACE, table=table))
    sql(INSERT_SQL.format(catalog=CATALOG, namespace=NAMESPACE, table="first", value=1))
    sql(INSERT_SQL.format(catalog=CATALOG, namespace=NAMESPACE, table="second", value=2))
    tables = ["first", "second"]
    path = root / "runs" / "20261011T090000Z-0a1b2c3d" / "pins.json"
    pinned = pins.current_snapshots(spark, CATALOG, NAMESPACE, tables)
    pins.write_once(path, pinned)
    written = pins.read_pins(path)
    results = {
        "pin writes every table with a numeric snapshot": sorted(written) == tables and all(value.isdigit() for value in written.values()),
        "verify passes while nothing moved": pins.differences(written, pins.current_snapshots(spark, CATALOG, NAMESPACE, tables)) == [],
        "a run is pinned once": refused(lambda: pins.write_once(path, pinned), "a run is pinned once"),
    }
    results["a writer that loses the race keeps the first pin"] = rival_keeps_its_pin(root / "runs" / "20261011T090000Z-1a2b3c4d" / "pins.json")
    sql(INSERT_SQL.format(catalog=CATALOG, namespace=NAMESPACE, table="first", value=3))
    moved = pins.differences(written, pins.current_snapshots(spark, CATALOG, NAMESPACE, tables))
    results["verify names the table that moved"] = len(moved) == 1 and moved[0].startswith("first: ")
    results["a missing table stops the pin"] = refused(lambda: pins.current_snapshots(spark, CATALOG, NAMESPACE, ["first", "absent"]), "absent cannot be read")
    results["a table with no snapshot stops the pin"] = refused(
        lambda: pins.current_snapshots(spark, CATALOG, NAMESPACE, ["empty"]), "empty has no current snapshot"
    )
    bad = root / "bad.json"
    bad.write_text('{"first": "not-a-number"}')
    results["a malformed pins file is refused"] = refused(lambda: pins.read_pins(bad), "must be a non-empty JSON object")
    results["a malformed run ID is refused"] = refused(lambda: pins.run_path("run_a"), "is not of the form")
    results["the committed table map names only plain identifiers"] = len(pins.mapped_tables()) > 0
    return results


def main() -> int:
    """Run the scenarios and write an outcomes-only report."""
    from scripts.lakehouse.session import spark_session

    root = Path(tempfile.mkdtemp(prefix="pins_e2e_"))
    spark = spark_session("pins-e2e", warehouse=root / "warehouse")
    try:
        results = scenarios(spark, root)
    finally:
        spark.stop()
        shutil.rmtree(root, ignore_errors=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report = {"run_at_utc": stamp, "inputs": "synthetic only, throwaway catalog", "scenarios": results, "passed": sum(results.values()), "total": len(results)}
    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    (REPORT_ROOT / f"report_{stamp}.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for name, passed in sorted(results.items()):
        sys.stdout.write(f"{'PASS' if passed else 'FAIL'} {name}\n")
    sys.stdout.write(f"{report['passed']} of {report['total']} scenarios passed\n")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
