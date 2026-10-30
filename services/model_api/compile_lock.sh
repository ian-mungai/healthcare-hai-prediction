#!/bin/sh
# Compile services/model_api/requirements.in into the hash-pinned requirements.txt, inside the pinned base image on linux/arm64.
# This script runs INSIDE the throwaway container (it starts no container and calls no Docker). The owner starts it from the repository root:
#
#   docker run --rm --platform linux/arm64 --memory 4g --user "$(id -u):$(id -g)" -e HOME=/tmp \
#     --volume "$PWD":/work --workdir /work \
#     python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3 \
#     sh services/model_api/compile_lock.sh
#
# It needs network access to PyPI only. pip-tools resolves in a temporary virtual environment of that image and writes every release file's hash.
# --only-binary=:all: makes a source-only package fail the compile, as it would fail the image build. Recompiling an unchanged source can pick up newer transitive
# releases, so review the diff of requirements.txt before committing.
# When the training image lock exists, the compile compares the model libraries with it and fails on a difference, so the API never loads a model with a library
# release other than the one that saved it. Compile the training lock first.
set -eu

PIP_TOOLS="pip-tools==7.6.1"
SOURCE="services/model_api/requirements.in"
LOCK="services/model_api/requirements.txt"
TRAINING_LOCK="services/training/requirements.txt"
SHARED="numpy pandas scipy scikit-learn statsmodels xgboost skops"
TOOLS="$(mktemp -d)"
BODY="$(mktemp)"
trap 'rm -rf "$TOOLS" "$BODY"' EXIT

python -m venv "$TOOLS/venv"
"$TOOLS/venv/bin/pip" install --quiet --disable-pip-version-check "$PIP_TOOLS"
"$TOOLS/venv/bin/pip-compile" --quiet --generate-hashes --allow-unsafe --strip-extras --no-header --no-emit-index-url \
    --pip-args "--only-binary=:all:" --output-file "$BODY" "$SOURCE"

if [ -f "$TRAINING_LOCK" ]; then
    for package in $SHARED; do
        ours="$(grep -i "^$package==" "$BODY" | cut -d ' ' -f 1)"
        theirs="$(grep -i "^$package==" "$TRAINING_LOCK" | cut -d ' ' -f 1)"
        if [ "$ours" != "$theirs" ]; then
            echo "model library differs from the training lock: serving '$ours', training '$theirs'" >&2
            exit 1
        fi
    done
else
    echo "note: $TRAINING_LOCK does not exist yet, so the model libraries were not compared with the training image" >&2
fi

{
    echo "# Model API image: FastAPI, uvicorn, the MLflow client 3.17.0, Feast 0.66.0 with its Redis online store, PyIceberg 0.12.0 and the model libraries with their"
    echo "# transitive packages. Every package is pinned with hashes (pip --require-hashes) for linux/arm64, resolved in the pinned python:3.12-slim-bookworm base"
    echo "# image with $PIP_TOOLS (services/model_api/compile_lock.sh)."
    cat "$BODY"
} >"$LOCK"
echo "compiled $LOCK ($(grep -c '^[A-Za-z0-9_.-]*==' "$LOCK") packages)"
