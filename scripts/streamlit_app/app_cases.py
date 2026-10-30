"""End-to-end cases of the Streamlit page, run through Streamlit's own test harness (``streamlit.testing.v1.AppTest``) against the stand-in API over real HTTP.

The harness runs ``app.py`` in the process and drives its widgets without a browser. It proves what the page shows for each answer of the API, that the limits
are on the page and that the key is on no element and in no session value. It does not prove the Streamlit server, the browser, the image or the configuration
file. These cases need Streamlit, which the repository's host environment does not install. They run in the page image (``run_streamlit_checks --require-app``).
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest import mock

from streamlit.testing.v1 import AppTest

from scripts.features.feast_harness import Checker
from scripts.streamlit_app import client, stand_in_api, view
from scripts.streamlit_app.stand_in_api import KEY, Behavior, StandInApi

APP = Path(__file__).resolve().parent / "app.py"
CCN, YEAR = "010001", 2024
TIMEOUT = 20


@contextmanager
def running(**env: str | None) -> Iterator[tuple[AppTest, StandInApi]]:
    """The page started with the stand-in's address and key. A value of None in ``env`` leaves that variable unset."""
    with StandInApi() as api:
        settings = {"MODEL_API_URL": api.url, "MODEL_API_KEY": KEY, "MODEL_API_DEADLINE_SECONDS": "2"} | env
        unset = [name for name, value in settings.items() if value is None]
        with mock.patch.dict(os.environ, {name: value for name, value in settings.items() if value is not None}):
            for name in unset:
                os.environ.pop(name, None)
            at = AppTest.from_file(str(APP), default_timeout=TIMEOUT)
            at.run()
            yield at, api


def predict(at: AppTest, ccn: str = CCN, year: int = YEAR) -> AppTest:
    at.text_input(key="ccn").set_value(ccn)
    at.number_input(key="window_year").set_value(year)
    at.button(key="predict").click()
    return at.run()


def shown(at: AppTest) -> list[str]:
    """Every text the page shows, whatever the element type, plus the session values."""
    elements: list[Any] = [*at.title, *at.markdown, *at.text, *at.info, *at.warning, *at.error, *at.success, *at.metric, *at.caption, *at.exception, *at.code]
    texts = [str(element.value) for element in elements] + [str(element.label) for element in at.metric]
    return texts + [str(at.session_state.filtered_state)]


def case_limits(c: Checker) -> None:
    with running() as (at, _):
        text = " ".join(shown(at)).lower()
        c.check("the page starts without an exception", not at.exception)
        for phrase in ("portfolio demonstration", "public data", "not clinical guidance", "not a clinical validation"):
            c.check(f"the page says {phrase!r} before any request", phrase in text)
        c.check("the page says it does not show model performance", "does not show how well the model performs" in text)
        c.check("nothing is predicted before the button", not at.metric and not at.error and not at.warning)
        c.check("the selectable HAI type is the configured one", at.selectbox(key="hai_type").options == ["HAI_1"])


def case_ok(c: Checker) -> None:
    with running() as (at, api):
        predict(at)
        c.check("no exception", not at.exception)
        c.check("the prediction is shown", [metric.value for metric in at.metric] == ["1.250"])
        lines = [text.value for text in at.text]
        c.check("the model version the API returned is shown", f"Model version: {stand_in_api.MODEL_VERSION}" in lines)
        c.check("the answering model is shown", "Answered by the stable model" in lines)
        c.check("no failure notice beside a prediction", not at.error and not at.warning)
        c.check("the API saw one call with the app's key", len(api.seen) == 1 and api.seen[0].headers.get("x-api-key") == KEY)


STATUS_TABLE: list[tuple[str, int, dict[str, str], str, str, str]] = [
    ("401 from a wrong key", 401, {}, "error", "The API refused the app's key", ""),
    ("413", 413, {}, "error", "The request was too large", ""),
    ("429 with Retry-After", 429, {"Retry-After": "7"}, "warning", "Too many requests", "7 seconds"),
    ("503 with Retry-After", 503, {"Retry-After": "10"}, "warning", "The API cannot serve the request now", "10 seconds"),
    ("404", 404, {}, "warning", "No data for this hospital and year", ""),
    ("500", 500, {}, "error", "Unexpected answer from the API", "500"),
]


def case_failures(c: Checker) -> None:
    for name, status, headers, level, title, detail in STATUS_TABLE:
        with running(**({"MODEL_API_KEY": "sk-test-another-key"} if status == 401 else {})) as (at, api):
            if status != 401:
                api.script(Behavior(status=status, headers=headers, body={"error": "x"}))
            predict(at)
            notices = [element.value for element in (at.error if level == "error" else at.warning)]
            c.check(f"{name}: a visible {level} with the right title", len(notices) == 1 and title in notices[0])
            c.check(f"{name}: the detail {detail!r} is shown", detail in " ".join(notices))
            c.check(f"{name}: no prediction is shown", not at.metric and not any("Model version" in text.value for text in at.text))
            c.check(f"{name}: no exception", not at.exception)
    with running(MODEL_API_DEADLINE_SECONDS="0.6") as (at, api):
        api.script(Behavior(hang=True))
        started = time.monotonic()
        predict(at)
        c.check("a stalled API ends in a visible timeout notice", len(at.error) == 1 and "did not answer within 0.6 seconds" in at.error[0].value)
        c.check("the page came back inside the harness timeout", time.monotonic() - started < TIMEOUT)
    with running(MODEL_API_URL=f"http://127.0.0.1:{stand_in_api.closed_port()}") as (at, _):
        predict(at)
        c.check("a closed port ends in a visible unreachable notice", len(at.error) == 1 and "could not be reached" in at.error[0].value)


def case_input_and_configuration(c: Checker) -> None:
    with running() as (at, api):
        predict(at, ccn="12")
        c.check("a short CCN is refused on the page", len(at.error) == 1 and view.CCN_HELP in at.error[0].value)
        c.check("a refused input sends nothing to the API", not api.seen)
    with running(MODEL_API_KEY=None) as (at, _):
        c.check("without a key the page says it is not configured", len(at.error) == 1 and "not configured" in at.error[0].value)
        c.check("without a key there is no button to press", not at.button)
        c.check("the configuration message names the variable, not a value", "MODEL_API_KEY is not set" in at.error[0].value)
    with running(MODEL_API_URL="http://api.example.test:8000") as (at, _):
        c.check("a plain http address that is not loopback is refused", len(at.error) == 1 and "loopback" in at.error[0].value)
    with running(STREAMLIT_HAI_TYPES="HAI_1,HAI_3") as (at, _):
        c.check("the HAI types come from the configuration", at.selectbox(key="hai_type").options == ["HAI_1", "HAI_3"])


def case_key_stays_private(c: Checker) -> None:
    with running() as (at, api):
        seen: list[str] = shown(at)
        predict(at)
        seen += shown(at)
        api.script(Behavior(status=500, body={"error": f"the key was {KEY}"}))
        predict(at)
        seen += shown(at)
        api.script(Behavior(body=b"junk" + KEY.encode()))
        predict(at)
        seen += shown(at)
    c.check("the sweep read the page four times", len(seen) >= 4)
    c.check("the key is on no element and in no session value", not any(KEY in text for text in seen))
    c.check("the key header name is on no element", not any("x-api-key" in text.lower() for text in seen))
    c.check(
        "the credential type is not stored in the session",
        not any(isinstance(value, client.Credential) for value in [*at.session_state.filtered_state.values()]),
    )


CASES: dict[str, tuple[Callable[[Checker], None], str]] = {
    "app_limits": (case_limits, "claims"),
    "app_ok": (case_ok, "api"),
    "app_failures": (case_failures, "api rate-limit unavailable timeout network"),
    "app_input_and_configuration": (case_input_and_configuration, "configuration input"),
    "app_key_stays_private": (case_key_stays_private, "secret"),
}
