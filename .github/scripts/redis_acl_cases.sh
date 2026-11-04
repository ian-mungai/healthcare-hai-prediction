#!/usr/bin/env bash
# Cases for the Redis start script that creates the read-only user of the model API (owner decision D18). The script comes from infra/services/redis.tf, exactly as the
# task runs it, and starts a real redis-server. Only the lines that name the network and the disk change: the server listens on a Unix socket in a scratch folder and
# writes nothing else. The ACL and password lines are untouched. Nothing leaves this machine. Run from the repository root; needs redis-server and redis-cli.
set -euo pipefail

tf="${REDIS_TF:-infra/services/redis.tf}"
failures=0
scratch="$(mktemp -d)"
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2> /dev/null || true
    wait "$server_pid" 2> /dev/null || true
  fi
}
trap 'cleanup; rm -r "$scratch"' EXIT

check() {
  local label="$1"
  shift
  if "$@"; then
    echo "ok: $label"
  else
    echo "FAILED: $label" >&2
    failures=$((failures + 1))
  fi
}

if ! command -v redis-server > /dev/null || ! command -v redis-cli > /dev/null; then
  echo "FAILED: redis-server and redis-cli are needed" >&2
  exit 1
fi

# The script of the task: the heredoc of redis_script_with_model_api_user, with the four spaces of its indentation removed and $${ turned into ${.
extract_script() {
  awk '/redis_script_with_model_api_user = <<-EOT/ {on = 1; next} on && /^  EOT$/ {exit} on {print}' "$tf" | sed -e 's/^    //' -e 's/[$][$][{]/${/g'
}

# A redis-server in front of the real one that changes only the network and disk lines.
stub="$scratch/bin"
mkdir -p "$stub"
real_server="$(command -v redis-server)"
cat > "$stub/redis-server" << STUB
#!/usr/bin/env bash
sed -e 's|^bind .*|bind 127.0.0.1|' -e 's|^port .*|port 0|' -e 's|^dir .*|dir $scratch|' -e '/^save /d' | { cat; printf '%s\n' 'unixsocket $scratch/redis.sock' 'unixsocketperm 700'; } | exec "$real_server" -
STUB
chmod +x "$stub/redis-server"

# Fresh random passwords with a quote, a backslash and a space in them, so the quoting of the start script is exercised. Nothing here is a real secret.
default_password="p\"w\\x y$RANDOM$RANDOM"
model_password="q\"u\\o z$RANDOM$RANDOM"
socket="$scratch/redis.sock"

script="$(extract_script)"
check "the task script was found in redis.tf" test -n "$script"

start_server() {
  PATH="$stub:$PATH" REDIS_PASSWORD="$default_password" MODEL_API_REDIS_PASSWORD="$model_password" bash -ec "$script" > "$scratch/server.log" 2>&1 &
  server_pid=$!
  local waited=0
  while ! REDISCLI_AUTH="$default_password" redis-cli -s "$socket" ping > /dev/null 2>&1; do
    waited=$((waited + 1))
    [[ $waited -le 100 ]] || { echo "FAILED: the server did not start in 10 seconds" >&2; cat "$scratch/server.log" >&2; exit 1; }
    sleep 0.1
  done
}
start_server

as_default() { REDISCLI_AUTH="$default_password" redis-cli -s "$socket" "$@" 2>&1; }
as_model() { REDISCLI_AUTH="$model_password" redis-cli -s "$socket" --user model_api "$@" 2>&1; }
# redis-cli reports a refused SELECT as "SELECT n failed: NOPERM ..." and then runs the command in database 0.
refused() { [[ "$1" == NOPERM* || "$1" == "SELECT "*" failed: NOPERM"* ]]; }

# --- the default user keeps everything it had --------------------------------------------------------------------------------------------------
check "the default user logs in with its password" test "$(as_default ping)" = "PONG"
check "the default user can write database 1" test "$(as_default -n 1 hset entity field value)" = "1"
check "the default user can write the marker in database 2" test "$(as_default -n 2 hset hai:fs:marker state ready)" = "1"
check "the default user can write database 0" test "$(as_default -n 0 set keep 1)" = "OK"
check "the default user cannot log in with the model API password" test "$(REDISCLI_AUTH="$model_password" redis-cli -s "$socket" ping 2>&1 | grep -c WRONGPASS)" = "1"

# --- the model API user reads what Feast and the marker reader read ---------------------------------------------------------------------------
check "the model API user logs in with its password" test "$(as_model ping)" = "PONG"
check "it reads a Feast hash in database 1" test "$(as_model -n 1 hmget entity field)" = "value"
check "it reads one field in database 1" test "$(as_model -n 1 hget entity field)" = "value"
check "it reads a whole hash in database 1" test "$(as_model -n 1 hgetall entity | tr '\n' ' ')" = "field value "
check "it reads the marker in database 2" test "$(as_model -n 2 hgetall hai:fs:marker | tr '\n' ' ')" = "state ready "
check "it checks that a key exists" test "$(as_model -n 1 exists entity)" = "1"
check "it sends its client information" test "$(as_model client setinfo lib-name feast)" = "OK"
check "it names its connection" test "$(as_model client setname model_api)" = "OK"

# --- and nothing else ---------------------------------------------------------------------------------------------------------------------------
# A connection starts in database 0 and an ACL cannot refuse that, so the user can read it. Nothing is meant to live there (online_protocol.py: "0 stays empty"),
# and the user cannot write it.
check "it cannot write database 0, which a connection starts in" refused "$(as_model -n 0 set keep changed)"
check "it cannot select a fourth database" refused "$(as_model -n 3 ping)"
check "it cannot write a hash" refused "$(as_model -n 1 hset entity field changed)"
check "it cannot set a string" refused "$(as_model -n 1 set key value)"
check "it cannot delete a key" refused "$(as_model -n 1 del entity)"
check "it cannot expire a key" refused "$(as_model -n 1 expire entity 1)"
check "it cannot flush a database" refused "$(as_model -n 1 flushdb)"
check "it cannot flush every database" refused "$(as_model flushall)"
check "it cannot list the keys" refused "$(as_model -n 1 keys '*')"
check "it cannot scan the keys" refused "$(as_model -n 1 scan 0)"
check "it cannot load a script" refused "$(as_model script load 'return 1')"
check "it cannot read the configuration" refused "$(as_model config get save)"
check "it cannot read the users" refused "$(as_model acl list)"
check "it cannot change the users" refused "$(as_model acl setuser model_api '+@all')"
check "it cannot read the server information" refused "$(as_model info)"
check "it cannot publish" refused "$(as_model publish channel message)"
check "it cannot shut the server down" refused "$(as_model shutdown nosave)"
check "it cannot save the data" refused "$(as_model save)"
check "it cannot rename a key" refused "$(as_model -n 1 rename entity other)"
check "the data it could not change is still there" test "$(as_default -n 1 hget entity field)" = "value"
check "database 0 still holds its key" test "$(as_default -n 0 get keep)" = "1"

# --- logins -------------------------------------------------------------------------------------------------------------------------------------
check "the model API user cannot log in with the shared Redis password" test "$(REDISCLI_AUTH="$default_password" redis-cli -s "$socket" --user model_api ping 2>&1 | grep -c WRONGPASS)" = "1"
check "the model API user cannot log in with a wrong password" test "$(REDISCLI_AUTH=wrong redis-cli -s "$socket" --user model_api ping 2>&1 | grep -c WRONGPASS)" = "1"
check "a user that does not exist cannot log in" test "$(REDISCLI_AUTH="$model_password" redis-cli -s "$socket" --user other ping 2>&1 | grep -c WRONGPASS)" = "1"
check "the user count is two: the default user and the model API user" test "$(as_default acl users | wc -l | tr -d ' ')" = "2"

# --- the passwords stay out of the server log -----------------------------------------------------------------------------------------------
check "the server log holds neither password" test -z "$(grep -F -e "$default_password" -e "$model_password" "$scratch/server.log" || true)"

[[ $failures -eq 0 ]]
