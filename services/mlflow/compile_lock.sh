#!/bin/sh
# Compile services/mlflow/requirements.in into the hash-pinned requirements.txt, inside the pinned base image on linux/arm64.
# This script runs INSIDE the throwaway container (it starts no container and calls no Docker). The lead starts it from the repository root:
#
#   docker run --rm --platform linux/arm64 --memory 4g --user "$(id -u):$(id -g)" -e HOME=/tmp \
#     --volume "$PWD":/work --workdir /work \
#     python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3 \
#     sh services/mlflow/compile_lock.sh
#
# Size --memory from the memory free at the time (4g is the floor the other lock compiles use). It needs network access to PyPI only.
# pip-tools resolves in a temporary virtual environment of that image and writes every release file's hash.
# --only-binary=:all: makes a source-only package fail the compile, as it would fail the image build. Recompiling an unchanged source can pick up
# newer transitive releases, so review the diff of requirements.txt before committing.
set -eu

PIP_TOOLS="pip-tools==7.6.1"
SOURCE="services/mlflow/requirements.in"
LOCK="services/mlflow/requirements.txt"
TOOLS="$(mktemp -d)"
BODY="$(mktemp)"
trap 'rm -rf "$TOOLS" "$BODY"' EXIT

python -m venv "$TOOLS/venv"
"$TOOLS/venv/bin/pip" install --quiet --disable-pip-version-check "$PIP_TOOLS"
"$TOOLS/venv/bin/pip-compile" --quiet --generate-hashes --allow-unsafe --strip-extras --no-header --no-emit-index-url \
    --pip-args "--only-binary=:all:" --output-file "$BODY" "$SOURCE"

{
    echo "# MLflow server image: MLflow 3.17.0, the PostgreSQL driver and boto3 with their transitive packages. Every package is pinned with hashes"
    echo "# (pip --require-hashes) for linux/arm64, resolved in the pinned python:3.12-slim-bookworm base image with $PIP_TOOLS (services/mlflow/compile_lock.sh)."
    cat "$BODY"
} >"$LOCK"
echo "compiled $LOCK ($(grep -c '^[A-Za-z0-9_.-]*==' "$LOCK") packages)"
