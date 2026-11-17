"""Cases for the Postgres exporter metrics that the dashboards and alerts name (review of 84b3722..7fc9030, finding 9).

The dashboard panel of the connection limit read ``pg_settings_max_connections`` while the exporter ran with ``--no-collector.settings``, so the panel stayed
empty and the offline file check could not tell. ``monitoring/postgres_exporter_metrics.txt`` records the metric families that the pinned exporter image printed
on loopback against a native PostgreSQL with exactly the flags of ``infra/services/monitoring.tf`` (the first line is the flag list). The cases require the
recorded flags to equal the Terraform flags, so a change of a flag needs a new recording, and require every ``pg_`` metric of a panel or a rule to be in the
record. A planted metric that the record lacks must be found, so the case can fail.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path

from scripts.infrastructure.job_identity_cases import expressions

RECORD = "monitoring/postgres_exporter_metrics.txt"
TERRAFORM = "infra/services/monitoring.tf"
PG_METRIC = re.compile(r"\bpg_[a-z0-9_]+")


def recorded(root: Path) -> tuple[str, set[str]]:
    lines = (root / RECORD).read_text(encoding="utf-8").splitlines()
    return lines[0].removeprefix("# flags: "), set(lines[1:])


def terraform_flags(root: Path) -> str:
    text = (root / TERRAFORM).read_text(encoding="utf-8")
    start = text.index('name                   = "postgres_exporter"')
    return " ".join(re.findall(r'"(--[^"]+)"', text[start : text.index("environment = [", start)]))


def names_in(expr: str, families: set[str]) -> set[str]:
    """The ``pg_`` metric names of an expression, each reduced to its family when it is a histogram or summary series."""
    return {re.sub(r"_(bucket|sum|count)$", "", name) if name not in families else name for name in PG_METRIC.findall(expr)}


def every_pg_metric_of_a_panel_or_a_rule_is_in_the_record(root: Path) -> dict[str, bool]:
    flags, families = recorded(root)
    used = {name for _, expr in expressions(root) for name in names_in(expr, families)}
    missing = sorted(used - families)
    return {
        "the record was made with the flags of the Terraform file": flags == terraform_flags(root),
        "the record has the connection metrics": {"pg_up", "pg_stat_database_numbackends"} <= families,
        "the panels and rules name at least four pg_ metrics": len(used) >= 4,
        "every pg_ metric of a panel or a rule is in the record": missing == [],
        "a metric that the record lacks is found": "pg_not_a_metric" not in families
        and names_in("pg_not_a_metric", families) - families == {"pg_not_a_metric"},
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "pg_every_pg_metric_of_a_panel_or_a_rule_is_in_the_record": every_pg_metric_of_a_panel_or_a_rule_is_in_the_record,
}
