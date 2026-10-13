# The one-time Polaris bootstrap image: the pinned apache/polaris-admin-tool image plus the deadline entrypoint of every finite job
# (D1 section 5 item 1). The tool image has no Python, so the entrypoint is the static Go program services/hai/deadline_run.go
# (standard library only), built here and copied in. The build context named "hai" is services/hai (see docker-compose.yaml).
FROM golang:1.25-alpine@sha256:1ae0735f00daffa3aaf1363a5184c0d2dc55c78e3db4ec70241cdac97bf84b59 AS deadline
COPY --from=hai deadline_run.go /src/deadline_run.go
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /deadline_run deadline_run.go

FROM apache/polaris-admin-tool:1.8.0@sha256:34dde19bb321aef5925a14ba90b69e26e982a5dcbda76a9d0cfc1aac1534ba4e
COPY --from=deadline --chmod=0755 /deadline_run /opt/hai/deadline_run
LABEL hai.deadline_entrypoint="1" hai.deadline_implementation="go"
