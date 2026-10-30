# Streamlit page image: the thin client of the model API. Build from the repository root, for Apple silicon, after the owner compiles the lock with
# services/streamlit/compile_lock.sh:
#   docker build --platform linux/arm64 -f services/streamlit/Dockerfile .
# The base image is pinned by digest and the wheels by hash; nothing is fetched at run time. The image holds only the files the page imports: no model, no feature
# store code and no lake access. It runs as an unprivileged user on a read-only root file system with a temporary folder mounted at /tmp.
FROM python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3

COPY services/streamlit/requirements.txt /opt/streamlit_app/requirements.txt
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: -r /opt/streamlit_app/requirements.txt

ENV PATH=/opt/venv/bin:${PATH} PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

RUN useradd --create-home --uid 10020 streamlitapp
WORKDIR /app

COPY --chmod=0444 scripts/streamlit_app/__init__.py scripts/streamlit_app/app.py scripts/streamlit_app/client.py scripts/streamlit_app/view.py /app/scripts/streamlit_app/
COPY --chmod=0444 services/streamlit/config.toml /app/.streamlit/config.toml

USER 10020
ENTRYPOINT ["streamlit", "run", "scripts/streamlit_app/app.py"]
