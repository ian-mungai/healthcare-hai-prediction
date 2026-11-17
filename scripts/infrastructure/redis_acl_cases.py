"""Cases for the ACL of the Redis exporter user (review of 84b3722..7fc9030, finding 1).

CONFIG GET returns ``requirepass`` in clear text, and the default user's password writes every key, so a user that may run CONFIG GET can become the default
user. The first case reads the rule list from ``infra/services/redis.tf`` and refuses any CONFIG, write, script or ACL command in it. The second case starts a
native ``redis-server`` on loopback with synthetic passwords and the very rule list of the Terraform file, logs in as the exporter user and shows what the
user can do: INFO, PING, CLIENT LIST, SLOWLOG, LATENCY, COMMAND INFO and DBSIZE work, and CONFIG GET and every write are refused. A planted copy of the old
list (with ``+config|get``) must show the leak, so the case can fail. The second case skips with a named reason when no ``redis-server`` is installed.
"""

from __future__ import annotations

import re
import shutil
import socket
from collections.abc import Callable
from pathlib import Path

from scripts.process import run_command

REDIS_FILE = "infra/services/redis.tf"
RULES = re.compile(r'redis_exporter_rules\s*=\s*"([^"]+)"')
APPROVED = {
    "resetkeys",
    "resetchannels",
    "-@all",
    "+info",
    "+ping",
    "+client|list",
    "+client|setname",
    "+slowlog|get",
    "+slowlog|len",
    "+latency|latest",
    "+latency|histogram",
    "+command|info",
    "+dbsize",
}
OLD_RULES = (
    "resetkeys resetchannels -@all +info +ping +config|get +client|list +client|setname +slowlog|get +slowlog|len +latency|latest +latency|histogram "
    "+command|info +dbsize"
)
ADMIN = "synthetic-admin-password"
LIMIT_SECONDS = 40.0
SCRIPT = r"""
set -u
dir=$(mktemp -d)
cd "$dir"
redis-server - >/dev/null 2>&1 <<CFG &
bind 127.0.0.1
port $HAI_PORT
dir $dir
save ""
maxmemory 64mb
requirepass "$HAI_ADMIN"
user exporter on ">synthetic-exporter-password" $HAI_RULES
CFG
server=$!
cleanup() { kill "$server" 2>/dev/null; wait "$server" 2>/dev/null; cd /; rm -rf "$dir"; }
trap cleanup EXIT
ask() { redis-cli -p "$HAI_PORT" --user exporter --pass synthetic-exporter-password --no-auth-warning "$@" 2>&1 | head -c 300 | tr '\n' ' '; }
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  case "$(ask ping)" in PONG*) break ;; esac
  sleep 0.25
done
echo "config=$(ask config get requirepass)"
echo "set=$(ask set key value)"
echo "hset=$(ask hset h f v)"
echo "eval=$(ask eval 'return 1' 0)"
echo "acl=$(ask acl list)"
echo "ping=$(ask ping)"
echo "dbsize=$(ask dbsize)"
raw() { redis-cli -p "$HAI_PORT" --user exporter --pass synthetic-exporter-password --no-auth-warning "$@" 2>&1 | tr -d '\r'; }
echo "info=$(raw info memory | grep -c '^maxmemory:')"
echo "client=$(ask client list | grep -c 'addr=')"
echo "slowlog=$(ask slowlog len)"
"""


def free_port() -> int:
    with socket.socket() as found:
        found.bind(("127.0.0.1", 0))
        return int(found.getsockname()[1])


def probe(rules: str) -> dict[str, str]:
    """What the exporter user gets back for each command, from a native server started with these rules."""
    env = {"PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin", "HAI_PORT": str(free_port()), "HAI_ADMIN": ADMIN, "HAI_RULES": rules}
    done = run_command("bash", ["-c", SCRIPT], env=env, timeout=LIMIT_SECONDS)
    return {name: value for name, _, value in (line.partition("=") for line in done.stdout.splitlines()) if name}


def the_rule_list_has_no_config_write_script_or_acl_command(root: Path) -> dict[str, bool]:
    found = RULES.search((root / REDIS_FILE).read_text(encoding="utf-8"))
    words = set(found.group(1).split()) if found else set()
    return {
        "the rule list is in the Terraform file": found is not None,
        "it is exactly the approved read-only list": words == APPROVED,
        "it has no CONFIG command": not any("config" in word.lower() for word in words),
        "it grants no command group and no key": not any(word.startswith("+@") or word.startswith("~") or word == "allkeys" for word in words),
    }


def the_exporter_user_cannot_read_the_config_or_write(root: Path) -> dict[str, bool]:
    if shutil.which("redis-server") is None or shutil.which("redis-cli") is None:
        return {"redis-server and redis-cli are not installed here, so the native check is skipped (the rule list case above still ran)": True}
    found = RULES.search((root / REDIS_FILE).read_text(encoding="utf-8"))
    rules = found.group(1) if found else ""
    shown = probe(rules)
    old = probe(OLD_RULES)
    text = " ".join(shown.values())
    return {
        "the server started and answered the exporter's login": shown.get("ping", "").startswith("PONG"),
        "CONFIG GET requirepass is refused": "NOPERM" in shown.get("config", "") and ADMIN not in text,
        "SET, HSET, EVAL and ACL are refused": all("NOPERM" in shown.get(name, "") for name in ("set", "hset", "eval", "acl")),
        "no reply holds the administrator password": ADMIN not in text,
        "INFO shows the memory limit that the dashboard needs": shown.get("info", "").strip() == "1",
        "DBSIZE, CLIENT LIST and SLOWLOG still work": shown.get("dbsize", "").strip() == "0"
        and shown.get("client", "").strip() != "0"
        and shown.get("slowlog", "").strip() == "0",
        "the planted old list leaks the password, so this case can fail": ADMIN in old.get("config", ""),
    }


CASES: dict[str, Callable[[Path], dict[str, bool]]] = {
    "redis_the_rule_list_has_no_config_write_script_or_acl_command": the_rule_list_has_no_config_write_script_or_acl_command,
    "redis_the_exporter_user_cannot_read_the_config_or_write": the_exporter_user_cannot_read_the_config_or_write,
}
