#!/usr/bin/env bash
set -euo pipefail

API_URL="${1:?api_url}"
RUN_ID="${2:?run_id}"
TARGET="${3:?target_status}"
TIMEOUT="${4:-120}"

# Prefer python, fall back to python3.
PY_BIN="${PY_BIN:-python}"
if ! command -v "${PY_BIN}" >/dev/null 2>&1; then
  PY_BIN="python3"
fi
command -v "${PY_BIN}" >/dev/null 2>&1 || {
  echo "❌ Neither 'python' nor 'python3' found. This script needs Python to parse JSON."
  exit 127
}

start="$(date +%s)"

while true; do
  # Don't let a transient curl failure kill the loop.
  resp="$(curl -sS "${API_URL}/demo/status/${RUN_ID}" || true)"

  status="$(
    printf '%s' "${resp}" | "${PY_BIN}" -c '
import json,sys
try:
  data = json.loads(sys.stdin.read() or "{}")
  print((data.get("status") or "UNKNOWN"))
except Exception:
  print("UNKNOWN")
' 2>/dev/null || true
  )"

  status="${status:-UNKNOWN}"
  echo "   status=${status}"

  if [ "${status}" = "${TARGET}" ]; then
    exit 0
  fi

  now="$(date +%s)"
  if (( now - start > TIMEOUT )); then
    echo "⏰ Timed out waiting for ${RUN_ID} to reach ${TARGET} (last=${status})"
    echo "Last response (truncated):"
    echo "${resp}" | head -c 600 || true
    echo
    exit 1
  fi

  sleep 2
done
