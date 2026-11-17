"""Register the ingestion timetables with Airflow, which loads a custom timetable class only through a plugin.

The scheduler, the DAG processor and the API server each load the plugins folder (``/opt/airflow/plugins``, the folder this one is mounted at), so each
can read the timetable of a serialized DAG back. The classes are in ``orchestration/ingestion_timetables.py``, which the same containers mount.
"""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from orchestration.ingestion_timetables import IngestionMonthlyTimetable, IngestionRetryTimetable


class HaiTimetablesPlugin(AirflowPlugin):
    name = "hai_timetables"
    timetables = [IngestionMonthlyTimetable, IngestionRetryTimetable]
