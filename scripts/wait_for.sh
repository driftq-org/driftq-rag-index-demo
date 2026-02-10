#!/usr/bin/env bash
set -euo pipefail

URL="${1:?url}"
TIMEOUT="${2:-60}"

start=$(date +%s)
while true; do
  if curl -fsS "${URL}" >/dev/null 2>&1; then
    exit 0
  fi
  now=$(date +%s)
  if (( now - start > TIMEOUT )); then
    echo "Timed out waiting for ${URL}"
    exit 1
  fi
  sleep 1
done
