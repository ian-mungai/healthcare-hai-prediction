# The Airflow image of the orchestration stack: the official apache/airflow 3.3.2 (Python 3.12) image pinned by digest, plus the deadline
# entrypoint of every finite job and its two labels. Nothing is installed: no package, no pip or uv call. The offline
# check (scripts/orchestration/offline_checks.py) compares every distribution in the built image with constraints-3.12.txt and the image's
# layers with this file. The entrypoint is the static Go program services/hai/deadline_run.go (standard library only), built here in a
# pinned golang stage, as services/lakehouse/polaris_admin/Dockerfile does for the Polaris admin tool.
#
# Build once from the repository root, with the base and builder images present (the digests below):
#   docker build --tag hai-airflow:3.3.2-python3.12 --build-context hai=services/hai services/orchestration
FROM golang:1.27.3-alpine@sha256:f2c872bcee949b7be9c490efd02b48a8a2fbf947cdf533f1c58525ec9d8170fd AS deadline
# hai is the services/hai named build context passed by the build command above.
# hadolint ignore=DL3022
COPY --from=hai deadline_run.go /src/deadline_run.go
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /deadline_run deadline_run.go

FROM apache/airflow@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1
COPY --from=deadline --chmod=0755 /deadline_run /opt/hai/deadline_run
LABEL hai.deadline_entrypoint="1" hai.deadline_implementation="go"
