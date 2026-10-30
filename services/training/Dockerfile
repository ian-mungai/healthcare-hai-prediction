# Training image: the model libraries of scripts/training, the MLflow client and DVC 3.67.1 with its S3 remote in its own environment at /opt/dvc.
# The code is not in the image. The first container of the task fetches the run's code archive (fetch_code.py) and links /opt/dvc into it.
# The base image is pinned by digest and the wheels by hash. Build for Apple silicon: docker build --platform linux/arm64 (see services/training/compile_lock.sh).
FROM python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3

# The OpenMP runtime that the XGBoost and scikit-learn wheels load.
# hadolint ignore=DL3008
RUN apt-get update \
    && apt-get install --no-install-recommends -y libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/training/requirements.txt
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: -r /opt/training/requirements.txt

# dvc is the scripts/quality/dvc named build context that images.json passes. It holds the DVC pins that scripts/quality/install_dvc.py installs. One package,
# antlr4-python3-runtime, is built from its hashed source archive after the hashed build tool is installed, as that script does.
# hadolint ignore=DL3022
COPY --from=dvc build_requirements.txt /opt/dvc_build/build_requirements.txt
# hadolint ignore=DL3022
COPY --from=dvc requirements.txt /opt/dvc_build/requirements.txt
RUN python -m venv /opt/dvc \
    && /opt/dvc/bin/pip install --no-cache-dir --require-hashes -r /opt/dvc_build/build_requirements.txt \
    && /opt/dvc/bin/pip install --no-cache-dir --require-hashes --no-build-isolation -r /opt/dvc_build/requirements.txt

COPY --chmod=0444 fetch_code.py /opt/hai/fetch_code.py

ENV PATH=/opt/venv/bin:${PATH}
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 MLFLOW_DISABLE_TELEMETRY=true DO_NOT_TRACK=true DVC_NO_ANALYTICS=1

# Run as an unprivileged user. The task definition mounts /workspace and /tmp as the only writable folders.
RUN useradd --create-home --uid 10001 training
USER 10001
WORKDIR /workspace
