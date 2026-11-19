# Model API image: FastAPI under uvicorn serving the registered models, with Feast online reads from Redis and the prediction append to Iceberg.
# Build from the repository root, for Apple silicon. services/model_api/requirements.txt is generated from the lock before each build:
# .venv/bin/python -m scripts.quality.lock_env images (the model_api group of uv.lock; Git ignores the file).
#   docker build --platform linux/arm64 -f services/model_api/Dockerfile .
# The base image is pinned by digest and the wheels by hash; nothing is fetched at run time. The image holds only the files the service imports and the seven
# model code files of the closure record. The models come from MLflow at run time as data only: no artifact code runs. It runs as an unprivileged user on a
# read-only root file system with a temporary folder mounted at /tmp.
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

COPY --chmod=0444 orchestration/__init__.py orchestration/training_contract.py /app/orchestration/
COPY --chmod=0444 scripts/features/__init__.py scripts/features/feast_registry_store.py scripts/features/online_protocol.py scripts/features/publication_reads.py scripts/features/retired_publications.py scripts/features/serving_contract_client.py /app/scripts/features/
COPY --chmod=0444 scripts/serving/__init__.py scripts/serving/api.py scripts/serving/body.py scripts/serving/canary.py scripts/serving/iceberg_sink.py scripts/serving/keys.py scripts/serving/limiter.py scripts/serving/main.py scripts/serving/manifests.py scripts/serving/metrics.py scripts/serving/mlflow_loader.py scripts/serving/predictions.py scripts/serving/service.py /app/scripts/serving/
# The model code that the trusted types of the model contract need: exactly the files of data_contracts/training/model_code.json (a case compares the two lists),
# its verifier and its record. The loader hashes these files at start and refuses to start when they differ from the record. skops loads a model's classes from here.
COPY --chmod=0444 scripts/model_code.py /app/scripts/
COPY --chmod=0444 scripts/training/__init__.py /app/scripts/training/
COPY --chmod=0444 scripts/training/models/__init__.py scripts/training/models/common.py scripts/training/models/elastic.py scripts/training/models/linear.py scripts/training/models/preprocessing.py scripts/training/models/trees.py /app/scripts/training/models/
COPY --chmod=0444 data_contracts/training/model_code.json /app/data_contracts/training/model_code.json
COPY --chmod=0444 data_contracts/features/retired_publications.json /app/data_contracts/features/retired_publications.json
COPY --chmod=0444 data_contracts/serving/predictions.json /app/data_contracts/serving/predictions.json
COPY --chmod=0444 data_contracts/training/model_contract.json /app/data_contracts/training/model_contract.json
COPY --chmod=0444 feature_store/feature_store_aws.yaml /app/feature_store/feature_store_aws.yaml

# COPY --chmod=0444 also gives the folders it creates mode 0444 (seen with Docker 29), which a non-root user cannot enter: make every folder under /app readable and enterable.
RUN find /app -type d -exec chmod 0555 {} +
USER 10010
ENTRYPOINT ["python", "-B", "-m", "scripts.serving.main"]
