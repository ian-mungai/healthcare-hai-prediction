#!/usr/bin/env bash
# Code checkout on the host: clear the apply lock and the launch hold that a failed services apply keeps.
#
# Check the apply and the services first. The status command below prints the apply lock line with the token of the lock that the failed apply holds. The script asks for what you
# checked and for that token and passes both as you typed them. Do not paste the command that the failed apply printed: it holds a placeholder where your statement belongs.
set -euo pipefail
trap 'echo "Stopped at line $LINENO. Read the output above and run the script again." >&2' ERR
cd "$(dirname "$0")/../../.."

.venv/bin/python -m scripts.infrastructure.services_apply status
printf 'What you checked: '
read -r RESOLVE_STATEMENT
printf 'Token of the failed apply lock: '
read -r LOCK_TOKEN
.venv/bin/python -m scripts.infrastructure.services_apply resolve --statement "$RESOLVE_STATEMENT" --token "$LOCK_TOKEN"
