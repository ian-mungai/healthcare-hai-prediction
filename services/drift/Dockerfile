# Drift image: Evidently 0.7.23, DuckDB 1.5.6 and jsonschema 4.26.0 with their transitive packages, for the check container of the drift task.
# The code is not in the image. The first container of the task fetches the run's code archive and the check phase runs from it (monitoring/aws_run.py).
# The base image is pinned by digest and the wheels by hash, and Evidently sends no usage data. Build for Apple silicon: docker build --platform linux/arm64
# (see services/drift/compile_lock.sh for the lock).
FROM python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3

COPY requirements.txt /opt/drift/requirements.txt
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: -r /opt/drift/requirements.txt

ENV PATH=/opt/venv/bin:${PATH}
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 DO_NOT_TRACK=1 EVIDENTLY_DISABLE_TELEMETRY=1

# Run as an unprivileged user. The task definition mounts /workspace and /tmp as the only writable folders.
RUN useradd --create-home --uid 10001 drift
USER 10001
WORKDIR /workspace
