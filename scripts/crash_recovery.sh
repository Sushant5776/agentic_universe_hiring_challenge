#!/usr/bin/env bash
# Demonstrate that a SIGKILL between chunks does not lose or double-count data.
set -euo pipefail

SIM_URL="${1:?usage: scripts/crash_recovery.sh <SIM_URL> [DB_PATH]}"
DB_PATH="${2:-state/crash_recovery.sqlite3}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
if [[ "$PYTHON_BIN" == "python3" && -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
fi
CRASH_AFTER_SECONDS="${CRASH_AFTER_SECONDS:-1}"

"$PYTHON_BIN" -m controller run --url "$SIM_URL" --db "$DB_PATH" &
controller_pid=$!
sleep "$CRASH_AFTER_SECONDS"
if kill -0 "$controller_pid" 2>/dev/null; then
  kill -9 "$controller_pid"
fi
wait "$controller_pid" 2>/dev/null || true

echo "controller killed; restarting against $DB_PATH" >&2
"$PYTHON_BIN" -m controller run --url "$SIM_URL" --db "$DB_PATH"
"$PYTHON_BIN" -m controller status --db "$DB_PATH" --json
