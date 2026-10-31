"""Import DAG files against a scratch ``orchestration`` package, with only the Airflow SDK stood in for. Started by ``airflow_mount_cases``.

    python -m scripts.orchestration.airflow_import_probe <scratch folder> <JSON list of DAG files>

The repository root is taken off the import path first, so the only ``orchestration`` package that a DAG can import is the one in the scratch folder.
The SDK and its providers are modules whose attributes are permissive mocks. The last output line is a JSON object of file name to error.
"""

from __future__ import annotations

import importlib.abc
import importlib.machinery
import importlib.util
import json
import sys
import types
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]


class Open(types.ModuleType):
    __path__: list[str] = []

    def __getattr__(self, name: str) -> mock.MagicMock:
        if name.startswith("__"):
            raise AttributeError(name)
        return mock.MagicMock(name=name)


class Finder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(self, name: str, path: object = None, target: object = None) -> importlib.machinery.ModuleSpec | None:
        return importlib.machinery.ModuleSpec(name, self, is_package=True) if name.split(".")[0] == "airflow" else None

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> types.ModuleType:
        return Open(spec.name)

    def exec_module(self, module: types.ModuleType) -> None:
        return None


def main(argv: list[str]) -> int:
    scratch, dags = argv[0], json.loads(argv[1])
    sys.path[:] = [entry for entry in sys.path if entry and Path(entry).resolve() != ROOT]
    sys.path.insert(0, scratch)
    sys.meta_path.insert(0, Finder())
    failed: dict[str, str] = {}
    for path in dags:
        name = Path(path).name
        spec = importlib.util.spec_from_file_location("dag_" + name[:-3], path)
        if spec is None or spec.loader is None:
            failed[name] = "cannot be loaded"
            continue
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as error:
            failed[name] = f"{type(error).__name__}: {error}"
    sys.stdout.write(json.dumps(failed) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
