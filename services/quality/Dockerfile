# Quality image: Great Expectations 1.23.2 reading the silver build file through DuckDB 1.5.6 (the engine that wrote it)
# and duckdb-engine. Kept apart from the analytics image so dbt does not carry pandas, SciPy and Altair.
# The base image is pinned by digest and the wheels by hash; nothing is fetched at validation time.
FROM python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3

COPY requirements.txt /opt/quality/requirements.txt
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: -r /opt/quality/requirements.txt

ENV PATH=/opt/venv/bin:${PATH}
# Great Expectations sends no usage data.
ENV GX_ANALYTICS_ENABLED=false

# Run as an unprivileged user; the code, suites and build file are mounted at run time.
RUN useradd --create-home --uid 10003 quality
USER quality
WORKDIR /workspace
