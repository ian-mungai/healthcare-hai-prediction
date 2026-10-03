#!/bin/sh
# Start the DuckDB UI with the bronze tables attached read-only, behind the loopback relay. Runs until the container stops.
# New cells default to the in-memory database: the UI switches to each default database's "main" schema, which the
# Iceberg catalog does not have, so query bronze tables by full name (lakehouse.bronze.<table>).
set -eu
python /opt/analytics/ui_relay.py &
tail -f /dev/null | duckdb -init /opt/analytics/bronze.sql -cmd "USE memory; LOAD ui; CALL start_ui_server();"
