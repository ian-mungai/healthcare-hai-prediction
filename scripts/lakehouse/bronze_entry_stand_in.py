"""Put service stand-ins under the real entry point ``python -m scripts.lakehouse.bronze``, for ``bronze_entry_cases``.

The case starts the real command in a child process with a ``sitecustomize.py`` on its path that calls ``install``. ``install`` runs before the command's
module loads. It replaces what the command reads from outside (``boto3``, the Spark session module) and nothing of ``bronze``, ``bronze_pin``,
``unit_records`` or ``dictionary``, so the command runs as ``__main__`` and the modules that it imports load their own copy of ``bronze``, exactly as in
production. The scenario comes from the environment: ``BRONZE_ENTRY_SCENARIO`` (JSON: ``objects``, the stored bytes as ``{"key": ..., "text": ...}``
rows) and ``BRONZE_ENTRY_RESULT`` (the file that receives the end state). For the plants, ``BRONZE_ENTRY_SOURCE`` names a file that loads in place of
``scripts/lakehouse/bronze.py``.
"""

from __future__ import annotations

import atexit
import importlib.abc
import importlib.machinery
import importlib.util
import io
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

SCENARIO = "BRONZE_ENTRY_SCENARIO"
RESULT = "BRONZE_ENTRY_RESULT"
SOURCE = "BRONZE_ENTRY_SOURCE"
MODULE = "scripts.lakehouse.bronze"


class SourceFinder(importlib.abc.MetaPathFinder):
    """Load ``MODULE`` from another file, for every import of it, including the one that ``python -m`` makes."""

    def __init__(self, source: str) -> None:
        self.source = source

    def find_spec(self, name: str, path: object = None, target: object = None) -> importlib.machinery.ModuleSpec | None:
        return importlib.util.spec_from_file_location(name, self.source) if name == MODULE else None


class Client:
    """The ``s3`` client: it serves the stored bytes of one scenario."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects

    def get_object(self, **keywords: str) -> dict[str, Any]:
        return {"Body": io.BytesIO(self.objects[keywords["Key"]])}


def install() -> None:
    """Set the stand-ins up for this process; called from ``sitecustomize``."""
    scenario = json.loads(os.environ[SCENARIO])
    client = Client({row["key"]: row["text"].encode() for row in scenario["objects"]})
    stops: list[bool] = []

    class Session:
        def __init__(self, **_keywords: object) -> None:
            pass

        def client(self, _service: str, **_keywords: object) -> Client:
            return client

    class Spark:
        def stop(self) -> None:
            stops.append(True)

    def spark_session(*_arguments: object, **_keywords: object) -> Spark:
        from scripts.lakehouse import bronze_metadata

        bronze_metadata.read = lambda *_: []  # no catalog: the previous generation is empty
        return Spark()

    stand_ins = {
        "boto3": SimpleNamespace(Session=Session),
        "scripts.lakehouse.session": SimpleNamespace(spark_session=spark_session, job_memory=lambda: "stand_in"),
    }
    mock.patch.dict(sys.modules, stand_ins).start()
    atexit.register(lambda: Path(os.environ[RESULT]).write_text(json.dumps({"spark_stops": len(stops)}), encoding="utf-8"))
    if SOURCE in os.environ:
        sys.meta_path.insert(0, SourceFinder(os.environ[SOURCE]))
