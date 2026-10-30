#!/bin/sh
# Run one long-lived command of the catalog task until the task's deadline, then stop it and exit 0.
# Usage: openmetadata_bounded_run.sh EXTRA_SECONDS COMMAND [ARGUMENT...]
# The init step writes the absolute deadline (epoch seconds) to OM_DEADLINE_FILE. The command may run until that time plus EXTRA_SECONDS, so the database and the
# search server outlive the catalog server by a short margin. A file at OM_ABORT_FILE ends the command at once with exit 1, so a failed bootstrap or migration
# does not keep the task paying until the deadline. The command's own exit status passes through when it ends first.
# ECS sends SIGTERM to the siblings of an essential container that ended. That is the normal end of the task when it arrives at or after the deadline, so the
# wrapper then stops its command and exits 0. A SIGTERM before the deadline means something else stopped the task, so the wrapper exits 1.
# The wrapper uses no tool beyond sh, date, sleep, kill and cat.
extra=$1
shift
deadline_file=${OM_DEADLINE_FILE:-/run/om/deadline}
abort_file=${OM_ABORT_FILE:-/run/om/abort}
grace=${OM_STOP_GRACE_SECONDS:-20}
poll=${OM_POLL_SECONDS:-5}

deadline=$(cat "$deadline_file") || {
  echo "openmetadata_bounded_run: no deadline file" >&2
  exit 1
}
limit=$((deadline + extra))
if [ "$(date +%s)" -ge "$limit" ]; then
  echo "openmetadata_bounded_run: the deadline has passed" >&2
  exit 1
fi

stopped=
trap 'stopped=1' TERM
"$@" &
child=$!
reason=
while kill -0 "$child" 2>/dev/null; do
  if [ -n "$stopped" ]; then
    reason=stopped
    break
  fi
  if [ -e "$abort_file" ]; then
    reason=abort
    break
  fi
  if [ "$(date +%s)" -ge "$limit" ]; then
    reason=deadline
    break
  fi
  sleep "$poll" &
  nap=$!
  wait "$nap"
  kill "$nap" 2>/dev/null
done

if [ -z "$reason" ]; then
  wait "$child"
  exit $?
fi

kill -TERM "$child" 2>/dev/null
waited=0
while kill -0 "$child" 2>/dev/null && [ "$waited" -lt "$grace" ]; do
  sleep 1
  waited=$((waited + 1))
done
kill -KILL "$child" 2>/dev/null
wait "$child" 2>/dev/null
if [ "$reason" = deadline ]; then
  exit 0
fi
if [ "$reason" = stopped ] && [ "$(date +%s)" -ge "$deadline" ]; then
  exit 0
fi
exit 1
