#!/usr/bin/env bash
set -euo pipefail

API_URL="${1:?api_url}"
RUN_ID="${2:?run_id}"
TARGET="${3:?target_status}"
TIMEOUT="${4:-120}"

start=$(date +%s)
while true; do
  status=$(curl -sS "${API_URL}/demo/status/${RUN_ID}" | python -c "import json,sys; print(json.load(sys.stdin).get('status'))")
  echo "   status=${status}"
  if [ "${status}" = "${TARGET}" ]; then
    exit 0
  fi
  now=$(date +%s)
  if (( now - start > TIMEOUT )); then
    echo "Timed out waiting for ${RUN_ID} to reach ${TARGET} (last=${status})"
    exit 1
  fi
  sleep 2
done
