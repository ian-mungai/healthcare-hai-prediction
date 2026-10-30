"""The serving read contract of the online feature store, as a reference client (design p3c section 4.4).

The prediction API must follow this contract; the online cases drive this client against the writer. For each request:

1. Read the marker hash. 503 unless ``state`` is ``ready`` and ``feature_schema_sha256`` equals the hash the model was trained with.
2. Build ``spine_key = ccn + ":" + window_year`` and read the model's feature references plus ``feature_matrix_hospital_window:has_pos_snapshot`` (a registered
   predictor that is never null for a published key), without duplicates, with event times.
3. Read the marker again. 503 unless it is ``ready`` with the same ``load_id`` as the first read; the values of step 2 are discarded otherwise.
4. 404 when the event time of ``feature_matrix_hospital_window`` is the epoch (a key never written, Feast fact F10). This holds for a model that uses no
   hospital-window predictor too, because the probe is always requested. A requested reference whose own view row is absent (no event time, or the epoch) is
   503: an absent row is not a value published as null, and the model must not receive it as one.
5. Return the values, ``features_as_of`` (the window start) and the schema hash. Nothing else from the marker is returned.
6. Every Redis call has a 1 s timeout and none is retried; a timeout or any Redis or Feast error is 503.

At start-up ``check_startup`` refuses a model whose references are not all in the schema file's servable list (Feast rejects unregistered references, fact F13).
Feast and redis-py are imported by the adapters only, when they are built.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from scripts.features import online_protocol as online

WINDOW_VIEW = "feature_matrix_hospital_window"
PROBE = f"{WINDOW_VIEW}:has_pos_snapshot"
CCN = re.compile(r"[A-Za-z0-9]{6}")
YEAR_MIN, YEAR_MAX = 1990, 2100
TIMESTAMP_POSTFIX = "__ts"  # Feast's OnlineResponse.to_dict(include_event_timestamps=True) suffix; the value is whole seconds


class ServingError(Exception):
    """A request the contract refuses; ``status`` is the HTTP status the API answers with."""

    status = 0


class Unavailable(ServingError):
    status = 503


class NotFound(ServingError):
    status = 404


class StartupRefused(RuntimeError):
    """The model's feature list names a reference that Feast does not serve."""


@dataclass(frozen=True)
class Served:
    values: dict[str, Any]
    features_as_of: str
    feature_schema_hash: str


MarkerReader = Callable[[], Mapping[str, str]]
FeatureReader = Callable[[list[str], str], Mapping[str, Sequence[Any]]]


def check_startup(references: Sequence[str], servable: Sequence[str]) -> None:
    """Refuse to start when a reference of the model's feature list is not servable (design 3.2 and 4.4)."""
    unknown = sorted(set(references) - set(servable))
    if unknown:
        raise StartupRefused(f"{len(unknown)} feature references are not servable ({', '.join(unknown[:5])})")


def spine_key(ccn: str, window_year: int) -> str:
    """The entity key of a validated request."""
    if not CCN.fullmatch(ccn) or isinstance(window_year, bool) or not isinstance(window_year, int) or not YEAR_MIN <= window_year <= YEAR_MAX:
        raise ValueError("the request is not a valid hospital-window")
    return f"{ccn}:{window_year}"


def requested(references: Sequence[str]) -> list[str]:
    """The model's references plus the probe, without duplicates, in first-seen order."""
    return list(dict.fromkeys([*references, PROBE]))


def feast_name(reference: str) -> str:
    """The key Feast uses for a reference when full feature names are on."""
    return reference.replace(":", "__", 1)


def checked_marker(raw: Mapping[str, str], schema_hash: str | None = None) -> Mapping[str, str]:
    """The marker, or Unavailable when it is not ``ready`` (or the schema hash differs)."""
    if raw.get("state") != "ready":
        raise Unavailable("the online store is not ready")
    if schema_hash is not None and raw.get("feature_schema_sha256") != schema_hash:
        raise Unavailable("the online store holds another feature schema than the model")
    return raw


def serve(read_marker: MarkerReader, read_features: FeatureReader, *, references: Sequence[str], schema_hash: str, ccn: str, window_year: int) -> Served:
    """One request under the contract; raises ``Unavailable`` (503) or ``NotFound`` (404), never a partial answer."""
    key = spine_key(ccn, window_year)
    try:
        first = checked_marker(read_marker(), schema_hash)
        wanted = requested(references)
        answer = read_features(wanted, key)
        second = read_marker()
    except ServingError:
        raise
    except Exception:
        raise Unavailable("the online store did not answer") from None
    if second.get("state") != "ready" or second.get("load_id") != first.get("load_id") or not first.get("load_id"):
        raise Unavailable("the online store changed during the read")
    try:
        probe = feast_name(PROBE)
        seconds = int(answer[probe + TIMESTAMP_POSTFIX][0])
        values = {reference: answer[feast_name(reference)][0] for reference in references}
    except (KeyError, IndexError, TypeError, ValueError):
        raise Unavailable("the online store answered without the requested features") from None
    if seconds == 0:
        raise NotFound("the hospital-window is not in the online store")
    absent = sorted(reference for reference in references if not event_time(answer, feast_name(reference)))
    if absent:
        raise Unavailable(f"the online store holds no row of {len(absent)} requested views for this hospital-window")
    return Served(values, datetime.fromtimestamp(seconds, UTC).date().isoformat(), schema_hash)


def event_time(answer: Mapping[str, Sequence[Any]], name: str) -> int:
    """The event time in seconds of a feature's view row, or 0 when the answer has none (the view row is absent)."""
    stamps = answer.get(name + TIMESTAMP_POSTFIX)
    try:
        return int(stamps[0]) if stamps is not None and stamps[0] is not None else 0
    except (IndexError, TypeError, ValueError):
        return 0


def marker_reader(environ: MutableMapping[str, str], *, client: online.RedisClient | None = None) -> MarkerReader:
    """A marker reader over database 2 with the serving timeout (1 s)."""
    control = client if client is not None else online.connect(environ, online.CONTROL_DB, timeout=online.SERVING_TIMEOUT)
    return lambda: online.read_marker(control)


def serving_client(store: Any, environ: Mapping[str, str]) -> Any:
    """Give a Feast store's online store one client with the serving settings (1 s timeouts, no retries) and return it, replacing whatever client it had.

    ``feature_store.yaml`` sets 5 s timeouts for the jobs and redis-py would retry three times by default. Feast keeps one client per online store in
    ``_client`` (Feast 0.66.0, ``RedisOnlineStore._get_client``), so the client is set before the first read and the store is asked for it again to prove it.
    """
    online_store = store._get_provider().online_store
    client = online.connect(environ, online.FEATURE_DB, timeout=online.SERVING_TIMEOUT, decode=False)
    online_store._client = client
    if online_store._get_client(store.config.online_store) is not client:
        raise online.OnlineError("the Feast online store does not use the serving client")
    return client


def feast_reader(store: Any, environ: Mapping[str, str]) -> FeatureReader:
    """A feature reader over a Feast store (online store on database 1): full feature names and event times, one entity row.

    Building the reader configures the store's client (``serving_client``), so no reader can run on the job settings; every read checks that the store still
    uses that client and answers 503 (through ``serve``) when it does not.
    """
    client = serving_client(store, environ)
    online_store = store._get_provider().online_store

    def read(references: list[str], key: str) -> Mapping[str, Sequence[Any]]:
        if online_store._get_client(store.config.online_store) is not client:
            raise online.OnlineError("the Feast online store no longer uses the serving client")
        answer: Mapping[str, Sequence[Any]] = store.get_online_features(features=references, entity_rows=[{"spine_key": key}], full_feature_names=True).to_dict(
            include_event_timestamps=True
        )
        return answer

    return read
