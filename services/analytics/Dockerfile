# Analytics image: DuckDB 1.5.6 (Python package and CLI) with the Iceberg, S3, AWS and UI extensions installed at build time.
# The base image is pinned by digest and the wheels by hash; extensions are fetched once here, never at query time.
FROM python:3.12-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3

COPY requirements.txt /opt/analytics/requirements.txt
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: -r /opt/analytics/requirements.txt

ENV PATH=/opt/venv/bin:${PATH}

# Run as an unprivileged user; its home holds the extensions, and the code and AWS profile are mounted at run time.
RUN useradd --create-home --uid 10002 analytics
USER analytics
RUN duckdb -c "INSTALL iceberg; INSTALL httpfs; INSTALL avro; INSTALL aws; INSTALL ui;" \
    && duckdb -c "COPY (SELECT extension_name, extension_version, install_mode FROM duckdb_extensions() WHERE installed AND extension_name IN ('iceberg', 'httpfs', 'avro', 'aws', 'ui') ORDER BY 1) TO '/home/analytics/extensions.csv'" \
    && cat /home/analytics/extensions.csv

COPY --chown=root:root bronze.sql ui.sh ui_relay.py /opt/analytics/
WORKDIR /workspace
