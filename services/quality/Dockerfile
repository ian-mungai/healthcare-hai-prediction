# Quality image: Great Expectations 1.23.2 reading the silver build file through DuckDB 1.5.6 (the engine that wrote it)
# and duckdb-engine. Kept apart from the analytics image so dbt does not carry pandas, SciPy and Altair.
# The base image is pinned by digest and the wheels by hash; nothing is fetched at validation time.
FROM golang:1.27.3-alpine@sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd AS deadline
# hai is the services/hai named build context passed by docker-compose.yaml additional_contexts.
# hadolint ignore=DL3022
COPY --from=hai deadline_run.go /src/deadline_run.go
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /deadline_run deadline_run.go

FROM python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3

COPY requirements.txt /opt/quality/requirements.txt
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: -r /opt/quality/requirements.txt

ENV PATH=/opt/venv/bin:${PATH}
# Great Expectations sends no usage data.
ENV GX_ANALYTICS_ENABLED=false

# Run as an unprivileged user; the code, suites and build file are mounted at run time.
RUN useradd --create-home --uid 10003 quality
USER 10003
WORKDIR /workspace

# The deadline entrypoint every finite job runs under (D1 section 5 item 1); the label lets the launcher refuse an image without it.
COPY --from=deadline --chmod=0755 /deadline_run /opt/hai/deadline_run
LABEL hai.deadline_entrypoint="1" hai.deadline_implementation="go"
