"""The feature sets of the features job: ``base`` and the model sets ``m1`` to ``m6``, and what each set publishes, reads and loads online.

``FEATURE_SET`` names the set a features job builds. ``all`` (or empty) means the single path that builds every table in one run.

- ``base`` publishes every table of the contract: the spine, the intermediate models, the label table, the build inputs and the six domain matrices. The
  domain matrices stay in base because the model sets are built from them (the ``ruca_zip`` mask of the pivot models reads gold's POS snapshot, which a
  model set run does not have) and because the training extract, the serving and monitoring code and the historical reader still read them.
- ``m1`` to ``m6`` publish one table each, ``feature_matrix_<set>``, built from the domain matrices of the newest committed base run
  (``newest_base_row``). The run records the base run it read as ``base_run`` in its publish record.

``online_action`` decides what the Feast phase does with a set: nothing for base, offline only for a set the rules keep offline, a held online load when the
set's schema hash differs from the hash of a Production model of that set, otherwise the online load. The Production models are the newest Production
record of each infection type that the promote action writes to the data bucket (``scripts.training.production_record``), so the features job needs no MLflow
access: a model belongs to a set when its record's ``feature_set`` names it, and its ``feature_set_schema_sha256`` is the hash it was trained on
(``feature_schema.set_schema_sha256``).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scripts.lakehouse.gold_publish_record import PublishError

if TYPE_CHECKING:
    from scripts.lakehouse.gold_publish import Plan

BASE = "base"
MODEL_SETS = ("m1", "m2", "m3", "m4", "m5", "m6")
SETS = (BASE, *MODEL_SETS)
VARIABLE = "FEATURE_SET"
SINGLE_PATH = "all"  # the value of the variable for the single path: a declared job variable needs a non-empty value
SPINE_KEY = "spine_key"
SET_TAG = "feature_set"
HASH_TAG = "feature_set_schema_sha256"
MODEL_CONTRACT = Path(__file__).resolve().parents[2] / "data_contracts/training/model_contract.json"
NONE, OFFLINE, LOAD, HOLD = "none", "offline", "load", "hold"


def checked(value: str | None) -> str:
    """The set a job names: empty (or ``all``) for the single path, else one of ``SETS``. Anything else is refused by value."""
    text = "" if value == SINGLE_PATH else (value or "")
    if text and text not in SETS:
        raise PublishError(f"{VARIABLE} {text!r} is not one of {SINGLE_PATH}, {', '.join(SETS)}")
    return text


def domain_tables(schema: Mapping[str, Any]) -> list[str]:
    """The domain matrices (the wide predictor tables), sorted: the tables a model set run reads from its base run."""
    return sorted(name for name, table in schema["tables"].items() if table["role"] == "predictor")


def set_tables(feature_set: str, contract_tables: Iterable[str], schema: Mapping[str, Any]) -> list[str]:
    """The tables a set publishes, sorted. ``contract_tables`` are the tables of the features contract."""
    feature_set = checked(feature_set)
    if feature_set in ("", BASE):
        return sorted(contract_tables)
    return [schema["model_sets"][feature_set]["table"]]


def scoped_plan(plan: Plan, feature_set: str, schema: Mapping[str, Any]) -> Plan:
    """The publish plan of one set: the contract's plan limited to the set's tables, the model tables keyed by the spine key."""
    feature_set = checked(feature_set)
    if feature_set in ("", BASE):
        return plan
    keys = {**dict(plan.tables), **{entry["table"]: (SPINE_KEY,) for entry in schema["model_sets"].values()}}
    return replace(plan, tables={name: keys[name] for name in set_tables(feature_set, plan.tables, schema)})


def newest_base_row(rows: Sequence[Mapping[str, Any]], base_tables: Iterable[str]) -> Mapping[str, Any] | None:
    """The newest committed row whose snapshot set names every base table. ``rows`` are in commit order, oldest first. A run of the single path counts."""
    wanted = set(base_tables)
    return next((row for row in reversed(rows) if wanted <= set(row["snapshot_set"])), None)


def production_models(records: Mapping[str, Mapping[str, Any] | None], feature_set: str) -> dict[str, str | None]:
    """The schema hash of each Production model that belongs to the set (None for a record without the hash), by infection type.

    ``records`` maps each infection type to its newest Production record, or None when no promotion wrote one."""
    return {hai_type: record.get(HASH_TAG) for hai_type, record in sorted(records.items()) if record is not None and record.get(SET_TAG) == feature_set}


def online_action(feature_set: str, schema: Mapping[str, Any], set_hash: str, production: Sequence[str | None]) -> str:
    """What the Feast phase does with the set: ``none`` (base), ``offline`` (a set that is not online), ``hold`` (a Production model of the set was trained on
    another schema) or ``load``. No Production model of the set means ``load``."""
    feature_set = checked(feature_set)
    if feature_set in ("", BASE):
        return NONE
    if not schema["model_sets"][feature_set]["online"]:
        return OFFLINE
    return HOLD if any(value != set_hash for value in production) else LOAD
