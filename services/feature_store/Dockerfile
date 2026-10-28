# Feature store image: Feast 0.66.0 with the DuckDB offline store (DuckDB 1.5.6), PyIceberg 0.12.0 reading the Iceberg features
# namespace and the Redis online store (redis-py 7.4.1, hiredis 3.4.2). The `aws` extra only brings boto3 for the S3 registry.
# The base image is pinned by digest and the wheels by hash; nothing is fetched at run time. Feast sends no usage data.
# Build for Apple silicon: docker build --platform linux/arm64 (see services/feature_store/requirements.in for the lock).
FROM golang:1.25-alpine@sha256:1ae0735f00daffa3aaf1363a5184c0d2dc55c78e3db4ec70241cdac97bf84b59 AS deadline
# hai is the services/hai named build context passed by docker-compose.yaml additional_contexts.
# hadolint ignore=DL3022
COPY --from=hai deadline_run.go /src/deadline_run.go
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /deadline_run deadline_run.go

FROM python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3

COPY requirements.txt /opt/feature_store/requirements.txt
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: -r /opt/feature_store/requirements.txt

ENV PATH=/opt/venv/bin:${PATH}
# The code, the Feast repository and the contracts are mounted read-only at run time, so Python writes no bytecode there.
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# Run as an unprivileged user; the run folder is the only writable mount.
RUN useradd --create-home --uid 10004 featurestore
USER 10004
WORKDIR /workspace

# The deadline entrypoint every finite job runs under (D1 section 5 item 1); the label lets the launcher refuse an image without it.
COPY --from=deadline --chmod=0755 /deadline_run /opt/hai/deadline_run
LABEL hai.deadline_entrypoint="1" hai.deadline_implementation="go"
