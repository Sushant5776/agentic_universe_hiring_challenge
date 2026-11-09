#!/usr/bin/env bash
set -euo pipefail
SIM_URL="${1:?usage: ./run.sh <SIM_URL>}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
if [[ "$PYTHON_BIN" == "python3" && -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
fi
exec "$PYTHON_BIN" -m controller run --url "$SIM_URL"
