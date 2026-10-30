# Model API image: FastAPI under uvicorn serving the registered models, with Feast online reads from Redis and the prediction append to Iceberg.
# Build from the repository root, for Apple silicon, after the owner compiles the lock with services/model_api/compile_lock.sh:
#   docker build --platform linux/arm64 -f services/model_api/Dockerfile .
# The base image is pinned by digest and the wheels by hash; nothing is fetched at run time. The image holds only the files the service imports. The models come
# from MLflow at run time and carry their own code. It runs as an unprivileged user on a read-only root file system with a temporary folder mounted at /tmp.
FROM python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3

# The OpenMP runtime that the XGBoost and scikit-learn wheels load.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install --no-install-recommends -y libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY services/model_api/requirements.txt /opt/model_api/requirements.txt
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: -r /opt/model_api/requirements.txt

ENV PATH=/opt/venv/bin:${PATH} PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 MLFLOW_DISABLE_TELEMETRY=true DO_NOT_TRACK=true

RUN useradd --create-home --uid 10010 modelapi
WORKDIR /app

COPY --chmod=0444 orchestration/__init__.py orchestration/serving_contract.py orchestration/training_contract.py /app/orchestration/
COPY --chmod=0444 scripts/features/__init__.py scripts/features/online_protocol.py scripts/features/publication_reads.py scripts/features/serving_contract_client.py /app/scripts/features/
COPY --chmod=0444 scripts/serving/__init__.py scripts/serving/api.py scripts/serving/canary.py scripts/serving/iceberg_sink.py scripts/serving/keys.py scripts/serving/limiter.py scripts/serving/main.py scripts/serving/manifests.py scripts/serving/mlflow_loader.py scripts/serving/predictions.py scripts/serving/service.py /app/scripts/serving/
COPY --chmod=0444 data_contracts/serving/predictions.json /app/data_contracts/serving/predictions.json
COPY --chmod=0444 data_contracts/training/model_contract.json /app/data_contracts/training/model_contract.json
COPY --chmod=0444 feature_store/feature_store_aws.yaml /app/feature_store/feature_store_aws.yaml

USER 10010
ENTRYPOINT ["python", "-B", "-m", "scripts.serving.main"]
