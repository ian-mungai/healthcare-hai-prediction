"""Check the ingestion timetables against Airflow's own timetable classes: the real cron times and the run IDs the scheduler would give.

Run it with an interpreter that has Airflow: the pinned image (through ``real_image_dags.py``) or a local install of Airflow 3.0 or later. ``.venv/bin/python``
has none. From the repository root: ``PYTHONPATH=. <python with airflow> -m scripts.orchestration.schedule_timetable_cases``. It prints one line
``TIMETABLE_CASES <json>`` and exits 1 when a case fails. A local Airflow older than the image has no ``airflow.sdk.TriggerRule``, which the DAG files
import, so ``--without-dag-files`` skips case 5 there.

What each case proves, with the scheduler's own calls (``next_dagrun_info`` for the next time of a schedule and ``generate_run_id`` for the name the scheduler
gives the run it creates, both of the real ``CronTriggerTimetable`` that the two timetables extend):

0. ``the_stock_timetable_names_a_run_scheduled``: the finding. Airflow's own timetable names the run of December 1 2026
   ``scheduled__2026-12-01T00:00:00+00:00``, while its job is ``ingestion_ab1ef6ef``.
1. ``the_cron_times_are_day_1_and_the_retry_days``: from November 2026 to April 2028 (a year end and a leap February) the monthly schedule gives midnight UTC on
   day 1 of every month and the retry schedule gives midnight UTC on days 2 and 3 of every month, the logical date being that time.
2. ``a_scheduled_run_has_the_run_id_of_its_job``: the name of each of those runs is the run ID that the job of the same logical date gets, written out here from
   its definition (``ingestion_<8 hex of the SHA-256 of the UTC time>``, ``ingestion_retry_<hex of day 1>_<day>``), is a canonical run ID, and is the ID the
   task resolves from the logical date.
3. ``a_hand_trigger_keeps_airflows_name``: a manual run still gets ``manual__<time>``.
4. ``the_timetables_survive_serialization_through_the_plugin``: a timetable is written as the scheduler stores it, read back and still names runs the same
   way, which needs ``airflow_plugins/hai_timetables.py`` to be loaded from the plugins folder.
5. ``the_production_dag_files_carry_the_timetables``: Airflow's own DAG parser, with the deployment named ``prod``, builds the two ingestion DAGs with their
   timetables, and with ``dev`` without them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
REPORT_PREFIX = "TIMETABLE_CASES "
FIRST = (2026, 11)
LAST = (2028, 4)


class CaseFailed(AssertionError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise CaseFailed(message)


def hex_of(moment: datetime) -> str:
    """The hex of a scheduled run, from its definition: eight digits of the SHA-256 of the UTC logical date."""
    return hashlib.sha256(moment.astimezone(UTC).isoformat().encode()).hexdigest()[:8]


def months() -> list[tuple[int, int]]:
    found, (year, month) = [], FIRST
    while (year, month) <= LAST:
        found.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return found


def walk(timetable: Any, count: int) -> list[Any]:
    """The next ``count`` runs of a timetable as the scheduler asks for them, starting before November 2026."""
    from airflow.timetables.base import TimeRestriction

    restriction = TimeRestriction(earliest=datetime(*FIRST, 1, tzinfo=UTC), latest=None, catchup=True)
    infos, last = [], None
    for _ in range(count):
        info = timetable.next_dagrun_info(last_automated_data_interval=last, restriction=restriction)
        infos.append(info)
        last = info.data_interval
    return infos


def run_name(timetable: Any, info: Any, run_type: Any = None) -> str:
    from airflow.utils.types import DagRunType

    return str(timetable.generate_run_id(run_type=run_type or DagRunType.SCHEDULED, run_after=info.run_after, data_interval=info.data_interval))


def expected_times() -> dict[str, list[datetime]]:
    monthly = [datetime(year, month, 1, tzinfo=UTC) for year, month in months()]
    retry = [datetime(year, month, day, tzinfo=UTC) for year, month in months() for day in (2, 3)]
    return {"monthly": monthly, "retry": retry}


def timetables() -> dict[str, Any]:
    from orchestration.ingestion_timetables import monthly_timetable, retry_timetable

    return {"monthly": monthly_timetable(), "retry": retry_timetable()}


def the_stock_timetable_names_a_run_scheduled() -> str:
    from airflow.timetables.trigger import CronTriggerTimetable

    stock = CronTriggerTimetable("0 0 1 * *", timezone="UTC")
    info = next(item for item in walk(stock, 18) if item.run_after.astimezone(UTC) == datetime(2026, 12, 1, tzinfo=UTC))
    name = run_name(stock, info)
    require(name == "scheduled__2026-12-01T00:00:00+00:00", f"Airflow's own timetable names the run {name}")
    require(name != f"ingestion_{hex_of(datetime(2026, 12, 1, tzinfo=UTC))}", "the stock name is already the name of the job")
    return name


def the_cron_times_are_day_1_and_the_retry_days() -> str:
    wanted = expected_times()
    for kind, timetable in timetables().items():
        infos = walk(timetable, len(wanted[kind]))
        got = [info.run_after.astimezone(UTC) for info in infos]
        require(got == wanted[kind], f"{kind}: the schedule gave {[item.isoformat() for item in got][:4]}..., not the times of the cron")
        logical = [info.logical_date.astimezone(UTC) for info in infos]
        require(logical == wanted[kind], f"{kind}: the logical date is not the time of the cron")
    return f"{len(wanted['monthly'])} monthly times and {len(wanted['retry'])} retry times"


def a_scheduled_run_has_the_run_id_of_its_job() -> str:
    from orchestration import ingestion_schedule as schedule
    from orchestration import training_contract as contract

    wanted = expected_times()
    seen: set[str] = set()
    for kind, timetable in timetables().items():
        for info in walk(timetable, len(wanted[kind])):
            moment = info.run_after.astimezone(UTC)
            first = moment.replace(day=1)
            expected = f"ingestion_{hex_of(moment)}" if kind == "monthly" else f"ingestion_retry_{hex_of(first)}_{moment.day}"
            name = run_name(timetable, info)
            require(name == expected, f"{kind} {moment.isoformat()}: the scheduler names the run {name}, its job gets {expected}")
            require(re.fullmatch(contract.RUN_ID, name) is not None and not name.startswith("scheduled__"), f"{name} is not a canonical run ID")
            context = {"dag_run": SimpleNamespace(run_type="scheduled", logical_date=info.logical_date)}
            if kind == "monthly":
                resolved = schedule.resolve_run_id(schedule.SCHEDULED_RUN, context)
            else:
                resolved = schedule.retry_run_id(*schedule.resolve_retry(schedule.SCHEDULED_RUN, schedule.SCHEDULED_RETRY_DAY, context))
            require(resolved == name, f"{kind} {moment.isoformat()}: the task resolves {resolved}, the run is {name}")
            require(name not in seen, f"two runs share the ID {name}")
            seen.add(name)
    return f"{len(seen)} run IDs, all equal to the ID of the job"


def a_hand_trigger_keeps_airflows_name() -> str:
    from airflow.utils.types import DagRunType

    for kind, timetable in timetables().items():
        info = walk(timetable, 1)[0]
        name = run_name(timetable, info, DagRunType.MANUAL)
        require(name.startswith("manual__"), f"{kind}: a manual run is named {name}")
    return "manual__<time>"


def encoders() -> tuple[Callable[[Any], Any], Callable[[Any], Any]]:
    try:
        from airflow.serialization.decoders import decode_timetable
        from airflow.serialization.encoders import encode_timetable
    except ImportError:
        from airflow.serialization.serialized_objects import decode_timetable, encode_timetable
    return encode_timetable, decode_timetable


def the_timetables_survive_serialization_through_the_plugin() -> str:
    encode, decode = encoders()
    wanted = expected_times()
    for kind, timetable in timetables().items():
        stored = encode(timetable)
        back = decode(stored)
        require(type(back) is type(timetable), f"{kind}: the timetable came back as {type(back).__name__}")
        info = walk(back, 1)[0]
        require(info.run_after.astimezone(UTC) == wanted[kind][0], f"{kind}: the stored timetable gives another first time")
        require(run_name(back, info).startswith("ingestion"), f"{kind}: the stored timetable names a run {run_name(back, info)}")
    return "both timetables round-trip as the scheduler stores them"


def the_production_dag_files_carry_the_timetables() -> str:
    """With the deployment named ``prod``, Airflow's own DAG parser builds the two ingestion DAGs with the timetables, and the DAG object is what the scheduler
    schedules: its timetable is the class that names a run as its job. With no environment named the DAGs have no schedule."""
    from unittest import mock

    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:
        from airflow.models.dagbag import DagBag

    for environment, schedules in (({"HAI_ENVIRONMENT": "prod"}, True), ({"HAI_ENVIRONMENT": "dev"}, False)):
        for file, dag_id, name in (
            ("ingestion_dag.py", "ingestion_dag", "IngestionMonthlyTimetable"),
            ("ingestion_retry_dag.py", "ingestion_retry_dag", "IngestionRetryTimetable"),
        ):
            with mock.patch.dict(os.environ, environment):
                bag = DagBag(dag_folder=str(ROOT / "dags" / file), include_examples=False)
            require(not bag.import_errors, f"{file} under {environment}: {bag.import_errors}")
            kind = type(bag.dags[dag_id].timetable).__name__
            require((kind == name) if schedules else (kind != name), f"{dag_id} under {environment} has the timetable {kind}")
    return "prod: both DAGs have their timetable; dev: neither"


CASES: dict[str, Callable[[], str]] = {
    "the_stock_timetable_names_a_run_scheduled": the_stock_timetable_names_a_run_scheduled,
    "the_cron_times_are_day_1_and_the_retry_days": the_cron_times_are_day_1_and_the_retry_days,
    "a_scheduled_run_has_the_run_id_of_its_job": a_scheduled_run_has_the_run_id_of_its_job,
    "a_hand_trigger_keeps_airflows_name": a_hand_trigger_keeps_airflows_name,
    "the_timetables_survive_serialization_through_the_plugin": the_timetables_survive_serialization_through_the_plugin,
    "the_production_dag_files_carry_the_timetables": the_production_dag_files_carry_the_timetables,
}


def run_cases(names: list[str] | None = None) -> list[dict[str, Any]]:
    results = []
    for name, case in CASES.items():
        if names is not None and name not in names:
            continue
        try:
            results.append({"case": name, "passed": True, "evidence": case()})
        except Exception as error:
            results.append({"case": name, "passed": False, "evidence": f"{type(error).__name__}: {error}", "trace": traceback.format_exc()[-1500:]})
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--without-dag-files", action="store_true", help="skip the case that parses the DAG files, which needs the Airflow SDK of the pinned image"
    )
    arguments = parser.parse_args(argv)
    os.environ.setdefault("AIRFLOW__CORE__PLUGINS_FOLDER", str(ROOT / "airflow_plugins"))
    sys.path.insert(0, str(ROOT))
    import airflow

    names = [name for name in CASES if not (arguments.without_dag_files and name == "the_production_dag_files_carry_the_timetables")]
    results = run_cases(names)
    sys.stdout.write(REPORT_PREFIX + json.dumps({"airflow": airflow.__version__, "cases": results}, sort_keys=True) + "\n")
    return 0 if all(item["passed"] for item in results) else 1


if __name__ == "__main__":
    sys.exit(main())
