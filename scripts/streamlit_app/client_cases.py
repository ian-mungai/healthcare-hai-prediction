"""End-to-end cases of the Streamlit client layer: the real client over real HTTP on the loopback address against the stand-in API.

The stand-in plays each answer the model API documents (200, 401, 404, 413, 422, 429, 503 with Retry-After) and the failures a network adds (a stalled
answer, a dripped answer, a closed port, a hostile body). The page itself runs in ``app_cases``. These cases prove the client and the words of the page, not
the real API, the real Streamlit server or a browser.
"""

from __future__ import annotations

import json
import logging
import pickle
import re
import time
from collections.abc import Callable
from typing import Any

from scripts.features.feast_harness import Checker
from scripts.streamlit_app import client, stand_in_api, view
from scripts.streamlit_app.stand_in_api import KEY, Behavior, StandInApi

CCN, YEAR = "010001", 2024
FORBIDDEN_WORDS = ("accurate", "accuracy", "reliable", "outperform", "validated", "auc", "rmse", "precision", "recall", "improve")


class LogCapture(logging.Handler):
    """Collect every log line of every logger while a case runs."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(self.format(record))

    def __enter__(self) -> LogCapture:
        self.previous = logging.getLogger().level
        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger().addHandler(self)
        return self

    def __exit__(self, *exc: object) -> None:
        logging.getLogger().removeHandler(self)
        logging.getLogger().setLevel(self.previous)


def settings_for(api: StandInApi, **overrides: str) -> client.Settings:
    env = {"MODEL_API_URL": api.url, "MODEL_API_KEY": KEY, "MODEL_API_DEADLINE_SECONDS": "2"} | overrides
    return client.load_settings(env)


def ask(api: StandInApi, **overrides: str) -> client.Prediction | client.Failure:
    return client.predict(settings_for(api, **overrides), "HAI_1", CCN, YEAR)


def answer(**changes: Any) -> dict[str, Any]:
    return stand_in_api.good_answer({"hai_type": "HAI_1", "ccn": CCN, "window_year": YEAR}, "r1") | changes


def case_ok(c: Checker) -> None:
    with StandInApi() as api:
        out = ask(api)
        seen = api.seen[0] if api.seen else None
    c.check("a good answer is a prediction", isinstance(out, client.Prediction))
    if not isinstance(out, client.Prediction) or seen is None:
        return
    c.check("the prediction value is the one the API gave", out.value == 1.25)
    c.check("the model version is the one the API returned", out.model_version == stand_in_api.MODEL_VERSION)
    c.check("the page can say which model answered", out.served_by == "stable" and out.fallback is False)
    c.check("one POST to the predict route", len(api.seen) == 1 and seen.method == "POST" and seen.path == "/v1/predict")
    c.check("the key travels in the key header", seen.headers.get("x-api-key") == KEY)
    c.check("the body holds exactly the three fields", json.loads(seen.body) == {"hai_type": "HAI_1", "ccn": CCN, "window_year": YEAR})
    c.check("a request id is sent and comes back", seen.headers.get("x-request-id") == out.request_id and len(out.request_id) >= 16)
    lines = view.prediction_lines(out)
    c.check("the page shows the model version", f"Model version: {stand_in_api.MODEL_VERSION}" in lines)
    c.check("the page shows the prediction", any("1.25" in line for line in lines))


STATUS_TABLE: list[tuple[str, int, dict[str, str], str, float | None]] = [
    ("413", 413, {}, "too_large", None),
    ("429 with Retry-After", 429, {"Retry-After": "7"}, "rate_limited", 7.0),
    ("429 without Retry-After", 429, {}, "rate_limited", None),
    ("503 with Retry-After", 503, {"Retry-After": "10"}, "unavailable", 10.0),
    ("503 without Retry-After", 503, {}, "unavailable", None),
    ("404", 404, {}, "not_found", None),
    ("422", 422, {}, "invalid_request", None),
    ("400", 400, {}, "invalid_request", None),
    ("408", 408, {}, "unexpected_status", None),
    ("500", 500, {}, "unexpected_status", None),
    ("redirect", 302, {"Location": "http://127.0.0.1:1/elsewhere"}, "unexpected_status", None),
]


def case_statuses(c: Checker) -> None:
    for name, status, headers, kind, wait in STATUS_TABLE:
        with StandInApi() as api:
            api.script(Behavior(status=status, headers=headers, body={"error": "x"}))
            out = ask(api)
            requests = len(api.seen)
        c.check(f"{name}: a failure of kind {kind}", isinstance(out, client.Failure) and out.kind == kind)
        if not isinstance(out, client.Failure):
            continue
        c.check(f"{name}: the status is kept", out.status == status)
        c.check(f"{name}: the wait is {wait}", out.retry_after == wait)
        c.check(f"{name}: one request only, nothing followed or retried", requests == 1)
        shown = view.notice(out)
        c.check(f"{name}: a visible notice with a title and text", shown.level in ("error", "warning") and bool(shown.title) and bool(shown.text))
        if kind == "unexpected_status":
            c.check(f"{name}: the notice names the status", str(status) in shown.text)
        if wait is not None:
            c.check(f"{name}: the notice names the wait", f"{wait:g} seconds" in shown.text)
        if kind in ("rate_limited", "unavailable") and wait is None:
            c.check(f"{name}: a missing header still gives a retry hint", "later" in shown.text.lower())
    with StandInApi() as api:
        out = ask(api, MODEL_API_KEY="sk-test-another-key")
        refusal = view.notice(out) if isinstance(out, client.Failure) else None
    c.check("a refused key is an unauthorized failure", isinstance(out, client.Failure) and out.kind == "unauthorized" and out.status == 401)
    c.check("the 401 notice is an error that points at the app's key", refusal is not None and refusal.level == "error" and "key" in refusal.text.lower())


def case_retry_after(c: Checker) -> None:
    table = [
        ("7", 7.0),
        ("0", 0.0),
        ("2.5", 2.5),
        ("99999", client.MAX_WAIT_SECONDS),
        ("-5", None),
        ("nan", None),
        ("inf", None),
        ("abc", None),
        ("", None),
        ("Wed, 21 Oct 2026 07:28:00 GMT", None),
    ]
    for value, wait in table:
        with StandInApi() as api:
            api.script(Behavior(status=429, headers={"Retry-After": value}, body={"error": "rate"}))
            out = ask(api)
        c.check(f"Retry-After {value!r} gives {wait}", isinstance(out, client.Failure) and out.retry_after == wait)


def timed(c: Checker, name: str, behavior: Behavior, deadline: str, limit: float) -> None:
    with StandInApi() as api:
        api.script(behavior)
        started = time.monotonic()
        out = ask(api, MODEL_API_DEADLINE_SECONDS=deadline)
        elapsed = time.monotonic() - started
    c.check(f"{name}: a timeout failure", isinstance(out, client.Failure) and out.kind == "timeout")
    c.check(f"{name}: back inside {limit} seconds (took {elapsed:.2f})", elapsed < limit)
    if isinstance(out, client.Failure):
        c.check(f"{name}: the notice says the API did not answer", "did not answer" in view.notice(out).text.lower())


def case_deadlines(c: Checker) -> None:
    timed(c, "a late answer", Behavior(delay=4.0, body=answer()), "0.6", 2.5)
    timed(c, "an API that never answers", Behavior(hang=True), "0.6", 2.5)
    timed(c, "an answer dripped one byte at a time", Behavior(drip=0.1, body=answer()), "0.8", 2.5)
    port = stand_in_api.closed_port()
    started = time.monotonic()
    out = client.predict(client.load_settings({"MODEL_API_URL": f"http://127.0.0.1:{port}", "MODEL_API_KEY": KEY}), "HAI_1", CCN, YEAR)
    c.check("a closed port is an unreachable failure", isinstance(out, client.Failure) and out.kind == "unreachable")
    c.check("a closed port answers at once", time.monotonic() - started < 3.0)
    if isinstance(out, client.Failure):
        c.check("the unreachable notice is visible", view.notice(out).level == "error")


def case_bad_answers(c: Checker) -> None:
    big = json.dumps(answer(extra="x" * 200_000)).encode()
    table: list[tuple[str, bytes | dict[str, Any]]] = [
        ("not JSON", b"not json"),
        ("a JSON list", b"[1, 2]"),
        ("a missing field", {key: value for key, value in answer().items() if key != "model_version"}),
        ("NaN", json.dumps(answer(prediction=float("nan"))).encode()),
        ("infinity", b'{"prediction": 1e999}'),
        ("a text number", answer(prediction="1.2")),
        ("a boolean number", answer(prediction=True)),
        ("another hospital", answer(ccn="999999")),
        ("another HAI type", answer(hai_type="HAI_2")),
        ("another year", answer(window_year=YEAR - 1)),
        ("a model version with a line break", answer(model_version="7\n# injected")),
        ("an empty model version", answer(model_version="")),
        ("an unknown served_by", answer(served_by="other")),
        ("a text fallback", answer(fallback="no")),
        ("an oversized body", big),
    ]
    for name, body in table:
        with StandInApi() as api:
            api.script(Behavior(body=body))
            out = ask(api)
        c.check(f"{name}: refused as a bad answer, nothing is shown as a prediction", isinstance(out, client.Failure) and out.kind == "bad_answer")
        if isinstance(out, client.Failure):
            c.check(f"{name}: the notice is visible", view.notice(out).level == "error")


def case_key_stays_private(c: Checker) -> None:
    hostile = Behavior(status=500, body={"error": f"your key was {KEY}"}, headers={"X-Echo": KEY})
    shown: list[str] = []
    with LogCapture() as logs, StandInApi() as api:
        settings = settings_for(api)
        outcomes = [ask(api)]
        for status in (401, 413, 429, 503, 404, 422):
            api.script(Behavior(status=status, headers={"Retry-After": "3"}, body={"error": KEY}))
            outcomes.append(ask(api))
        api.script(hostile)
        outcomes.append(ask(api))
        api.script(Behavior(body=b"junk" + KEY.encode()))
        outcomes.append(ask(api))
        api.script(Behavior(hang=True))
        outcomes.append(ask(api, MODEL_API_DEADLINE_SECONDS="0.5"))
        for outcome in outcomes:
            shown += [repr(outcome), str(outcome)]
            if isinstance(outcome, client.Failure):
                page = view.notice(outcome)
                shown += [page.title, page.text, repr(page)]
            else:
                shown += view.prediction_lines(outcome)
        shown += [repr(settings), str(settings), str(settings.api_key), repr(settings.api_key), f"{settings.api_key}", f"{settings!r}"]
    c.check("the sweep covered ten outcomes", len(outcomes) == 10)
    c.check("the client logged its calls", bool(logs.lines))
    c.check("no outcome, notice or settings text holds the key", not any(KEY in text for text in shown))
    c.check("no log line holds the key", not any(KEY in line for line in logs.lines))
    c.check("no log line holds the key header name", not any("x-api-key" in line.lower() for line in logs.lines))
    try:
        pickle.dumps(settings.api_key)
        stored = True
    except TypeError:
        stored = False
    c.check("the credential cannot be pickled, so it cannot enter a session store", not stored)
    bad = KEY + "\nX-Evil: 1"
    try:
        client.load_settings({"MODEL_API_KEY": bad})
        message = None
    except client.ConfigError as error:
        message = str(error)
    c.check("a key with a line break is refused without printing it", message is not None and KEY not in message and "X-Evil" not in message)


SETTINGS_REFUSED: list[tuple[str, dict[str, str]]] = [
    ("no key", {}),
    ("an empty key", {"MODEL_API_KEY": ""}),
    ("a key with a space", {"MODEL_API_KEY": "a b"}),
    ("a key with a tab", {"MODEL_API_KEY": "a\tb"}),
    ("a key over 256 characters", {"MODEL_API_KEY": "k" * 257}),
    ("a plain http address that is not loopback", {"MODEL_API_KEY": KEY, "MODEL_API_URL": "http://api.example.test:8000"}),
    ("an address with a login", {"MODEL_API_KEY": KEY, "MODEL_API_URL": "http://user:pw@127.0.0.1:8000"}),
    ("an ftp address", {"MODEL_API_KEY": KEY, "MODEL_API_URL": "ftp://127.0.0.1:8000"}),
    ("an address with a path", {"MODEL_API_KEY": KEY, "MODEL_API_URL": "http://127.0.0.1:8000/v1"}),
    ("an address with a query", {"MODEL_API_KEY": KEY, "MODEL_API_URL": "http://127.0.0.1:8000/?a=1"}),
    ("a zero deadline", {"MODEL_API_KEY": KEY, "MODEL_API_DEADLINE_SECONDS": "0"}),
    ("a deadline over the cap", {"MODEL_API_KEY": KEY, "MODEL_API_DEADLINE_SECONDS": "1000"}),
    ("a text deadline", {"MODEL_API_KEY": KEY, "MODEL_API_DEADLINE_SECONDS": "soon"}),
    ("a NaN deadline", {"MODEL_API_KEY": KEY, "MODEL_API_DEADLINE_SECONDS": "nan"}),
    ("an unknown HAI type", {"MODEL_API_KEY": KEY, "STREAMLIT_HAI_TYPES": "HAI_9"}),
    ("a repeated HAI type", {"MODEL_API_KEY": KEY, "STREAMLIT_HAI_TYPES": "HAI_1,HAI_1"}),
    ("an empty HAI list", {"MODEL_API_KEY": KEY, "STREAMLIT_HAI_TYPES": ""}),
]


def case_settings(c: Checker) -> None:
    default = client.load_settings({"MODEL_API_KEY": KEY})
    c.check("the default address is the host loopback of the API", default.api_url == "http://127.0.0.1:8000")
    c.check("the default deadline is 30 seconds", default.deadline_seconds == 30.0)
    c.check("the default HAI types are HAI_1", default.hai_types == ("HAI_1",))
    for name, env in SETTINGS_REFUSED:
        try:
            client.load_settings(env)
            refused, message = False, ""
        except client.ConfigError as error:
            refused, message = True, str(error)
        c.check(f"{name} is refused", refused)
        c.check(f"{name}: the refusal does not print the key", KEY not in message)
    accepted = [
        ("https://api.example.test:8443", "https"),
        ("http://localhost:8000", "http"),
        ("http://[::1]:8000", "http"),
        ("http://127.0.0.1:8000/", "http"),
    ]
    for url, scheme in accepted:
        try:
            result = client.load_settings({"MODEL_API_KEY": KEY, "MODEL_API_URL": url})
            ok = result.api_url.startswith(scheme)
        except client.ConfigError:
            ok = False
        c.check(f"{url} is accepted", ok)
    many = client.load_settings({"MODEL_API_KEY": KEY, "STREAMLIT_HAI_TYPES": "HAI_1,HAI_3"})
    c.check("several HAI types are listed in order", many.hai_types == ("HAI_1", "HAI_3"))


def case_words(c: Checker) -> None:
    limits = " ".join(view.LIMITS).lower()
    for phrase in ("portfolio demonstration", "public data", "not clinical guidance", "not a clinical validation"):
        c.check(f"the page says {phrase!r}", phrase in limits)
    failures = [client.Failure(kind=kind, status=500, retry_after=5.0, request_id="r", deadline_seconds=30.0) for kind in KINDS]
    texts = [limits] + [f"{view.notice(failure).title} {view.notice(failure).text}".lower() for failure in failures]
    texts += [line.lower() for line in view.prediction_lines(sample_prediction())]
    texts += [view.INTRO.lower()]
    for word in FORBIDDEN_WORDS:
        c.check(f"no page text claims {word!r}", not any(re.search(rf"\b{word}\b", text) for text in texts))
    c.check("every failure kind has its own notice", len({view.notice(failure).title for failure in failures}) == len(KINDS))
    c.check("the page says it does not show model performance", "does not show how well the model performs" in view.INTRO)
    table = [
        (CCN, YEAR, False),
        ("AB12CD", 1990, False),
        ("12345", YEAR, True),
        ("1234567", YEAR, True),
        ("01 001", YEAR, True),
        ("0100é1", YEAR, True),
        (CCN, 1989, True),
        (CCN, 2101, True),
    ]
    for ccn, year, problem in table:
        c.check(f"input {ccn!r} {year}: {'a problem' if problem else 'accepted'}", (view.input_problem(ccn, year) is not None) == problem)


KINDS = (
    "unauthorized",
    "too_large",
    "rate_limited",
    "unavailable",
    "timeout",
    "unreachable",
    "not_found",
    "invalid_request",
    "bad_answer",
    "unexpected_status",
)


def sample_prediction() -> client.Prediction:
    return client.Prediction(
        request_id="r1",
        hai_type="HAI_1",
        ccn=CCN,
        window_year=YEAR,
        value=1.25,
        model_version="7",
        served_by="stable",
        fallback=False,
        features_as_of="2026-10-01",
        feature_schema_sha256="ab" * 32,
    )


CASES: dict[str, tuple[Callable[[Checker], None], str]] = {
    "client_ok": (case_ok, "api"),
    "client_statuses": (case_statuses, "api rate-limit unavailable"),
    "client_retry_after": (case_retry_after, "api rate-limit"),
    "client_deadlines": (case_deadlines, "timeout network"),
    "client_bad_answers": (case_bad_answers, "api malformed"),
    "client_key_stays_private": (case_key_stays_private, "secret logging"),
    "client_settings": (case_settings, "configuration secret"),
    "page_words": (case_words, "claims"),
}
