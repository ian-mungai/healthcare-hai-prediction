"""The Streamlit page: enter a hospital and a year, get the model API's prediction.

The page is a thin client. It gets predictions only through ``client.predict``, with the key from the environment that the container's secret store injects.
It reads no model, no feature store and no lake table. It keeps nothing in the session: the key is not in any widget, and the answer lives for one run.
Start it with ``streamlit run scripts/streamlit_app/app.py`` from the repository root.
"""

from __future__ import annotations

import logging
import os

import streamlit as st

from scripts.streamlit_app import client, view

logger = logging.getLogger("streamlit_app")
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)


def show_failure(failure: client.Failure) -> None:
    notice = view.notice(failure)
    getattr(st, notice.level)(f"**{notice.title}**\n\n{notice.text}")


def main() -> None:
    st.set_page_config(page_title=view.TITLE, layout="centered")
    st.title(view.TITLE)
    for limit in view.LIMITS:
        st.info(limit)
    st.markdown(view.INTRO)
    try:
        settings = client.load_settings(os.environ)
    except client.ConfigError as error:
        st.error(f"The app is not configured: {error}.")
        st.stop()
    hai_type = st.selectbox("HAI type", settings.hai_types, key="hai_type")
    ccn = st.text_input("Hospital CCN", max_chars=6, help=view.CCN_HELP, key="ccn")
    window_year = int(st.number_input("Window year", min_value=client.YEAR_MIN, max_value=client.YEAR_MAX, value=2024, step=1, key="window_year"))
    if not st.button("Predict", key="predict"):
        return
    ccn = ccn.strip()
    problem = view.input_problem(ccn, window_year)
    if problem is not None:
        st.error(problem)
        return
    with st.spinner("Asking the model API"):
        outcome = client.predict(settings, str(hai_type), ccn, window_year)
    if isinstance(outcome, client.Failure):
        show_failure(outcome)
        return
    st.metric("Predicted standardized infection ratio", view.value_text(outcome))
    for line in view.prediction_lines(outcome)[1:]:
        st.text(line)


main()
