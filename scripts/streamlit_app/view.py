"""The words of the Streamlit page, kept apart from Streamlit so the cases can check them.

The page claims no model performance: it shows only what the API returned for the hospital-window, with the model version the API gave.
"""

from __future__ import annotations

from dataclasses import dataclass

from scripts.streamlit_app.client import CCN_PATTERN, YEAR_MAX, YEAR_MIN, Failure, Prediction

TITLE = "HAI prediction demonstration"

LIMITS: tuple[str, ...] = (
    "This is a portfolio demonstration on public data. It is not clinical guidance and not a clinical validation.",
    "Do not use a prediction for any care or safety decision. The page holds no patient data.",
    "The hospital data come from public Centers for Medicare & Medicaid Services (CMS) hospital reports.",
)

INTRO = (
    "The page asks the model API for the predicted standardized infection ratio (SIR) of one hospital in one year window. "
    "An SIR is the number of infections a hospital reports divided by the number expected from national baseline data for hospitals like it. "
    "The page does not show how well the model performs. It shows only what the API returns for the hospital-window you enter."
)

CCN_HELP = "Enter the six character CMS Certification Number (CCN) of the hospital, letters and digits only."


@dataclass(frozen=True)
class Notice:
    level: str
    title: str
    text: str


def seconds(value: float) -> str:
    return f"{value:g} second" + ("" if value == 1 else "s")


def retry_hint(wait: float | None) -> str:
    return f"Try again in {seconds(wait)}." if wait is not None else "Try again later."


def notice(failure: Failure) -> Notice:
    """The visible notice for one failure. Every kind has its own title, and the request id is there for the owner's log search."""
    status = failure.status
    reference = f" Request id: {failure.request_id}." if failure.request_id else ""
    kinds = {
        "unauthorized": (
            "error",
            "The API refused the app's key",
            f"The API answered {status}. The key this app holds is missing, wrong or retired. Ask the owner to check the app's key. Nothing was predicted.",
        ),
        "too_large": (
            "error",
            "The request was too large",
            f"The API answered {status}: the request was over its size limit. The page sends only three short fields, so report this. Nothing was predicted.",
        ),
        "rate_limited": (
            "warning",
            "Too many requests",
            f"The API answered {status}: this app is over its request rate. {retry_hint(failure.retry_after)}",
        ),
        "unavailable": (
            "warning",
            "The API cannot serve the request now",
            f"The API answered {status}: its feature store may be loading, or the service is busy. {retry_hint(failure.retry_after)}",
        ),
        "timeout": (
            "error",
            "The API did not answer in time",
            f"The API did not answer within {seconds(failure.deadline_seconds)}. Nothing was predicted. Try again later.",
        ),
        "unreachable": (
            "error",
            "The API could not be reached",
            "The page could not open a connection to the model API, or the connection broke. Check that the API service is running. Nothing was predicted.",
        ),
        "not_found": (
            "warning",
            "No data for this hospital and year",
            f"The API answered {status}: its online store holds no row for this hospital-window. Check the CCN and the year.",
        ),
        "invalid_request": (
            "error",
            "The API refused the request",
            f"The API answered {status}: the request was not valid. Check the CCN and the year.",
        ),
        "bad_answer": (
            "error",
            "The answer could not be used",
            "The API answered, but not in the documented form, so the page shows nothing from it.",
        ),
    }
    level, title, text = kinds.get(failure.kind, ("error", "Unexpected answer from the API", f"The API answered with status {status}. Nothing was predicted."))
    return Notice(level, title, text + reference)


def value_text(prediction: Prediction) -> str:
    return f"{prediction.value:.3f}"


def prediction_lines(prediction: Prediction) -> list[str]:
    """The lines under the prediction: the model version the API returned first, then which model answered and the data date."""
    answered = f"Answered by the {prediction.served_by} model" + (" (fell back from the canary model)" if prediction.fallback else "")
    return [
        f"Predicted standardized infection ratio: {value_text(prediction)}",
        f"Model version: {prediction.model_version}",
        answered,
        f"HAI type: {prediction.hai_type}, hospital (CCN): {prediction.ccn}, window year: {prediction.window_year}",
        f"Features as of: {prediction.features_as_of}",
        f"Feature schema hash: {prediction.feature_schema_sha256[:12]}",
        f"Request id: {prediction.request_id}",
    ]


def input_problem(ccn: str, window_year: int) -> str | None:
    """The reason the input cannot be sent, or None. The API checks again."""
    if not CCN_PATTERN.fullmatch(ccn):
        return CCN_HELP
    if not YEAR_MIN <= window_year <= YEAR_MAX:
        return f"The window year must be from {YEAR_MIN} to {YEAR_MAX}."
    return None
