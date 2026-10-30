# MLflow tracking server image: MLflow 3.17.0 with the PostgreSQL driver for the backend store and boto3 for the S3 artifact destination.
# The base image is pinned by digest and the wheels by hash; nothing is fetched at run time. Telemetry stays off.
# Build for Apple silicon: docker build --platform linux/arm64 (see services/mlflow/compile_lock.sh for the lock).
FROM python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3

COPY requirements.txt /opt/mlflow/requirements.txt
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: -r /opt/mlflow/requirements.txt

COPY --chmod=0444 start_server.py /opt/mlflow/start_server.py

ENV PATH=/opt/venv/bin:${PATH}
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 MLFLOW_DISABLE_TELEMETRY=true DO_NOT_TRACK=true

# Run as an unprivileged user. The task definition runs /opt/mlflow/start_server.py with the server options, a read-only root and a temporary /tmp.
# The script starts the server with basic authentication and no default permission.
RUN useradd --create-home --uid 10005 mlflow
USER 10005
WORKDIR /opt/mlflow
EXPOSE 5100
