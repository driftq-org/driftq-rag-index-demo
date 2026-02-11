#!/usr/bin/env bash
set -euo pipefail

URL="${1:?url}"
TIMEOUT="${2:-60}"

start="$(date +%s)"
last_log="$start"

while true; do
  # Fast check: fail quickly if it can't connect
  if curl -fsS --connect-timeout 2 --max-time 5 "${URL}" >/dev/null 2>&1; then
    exit 0
  fi

  now="$(date +%s)"

  # Log progress every ~10s so it doesn't look stuck
  if (( now - last_log >= 10 )); then
    # Best-effort show status code
    code="$(curl -sS -o /dev/null -w "%{http_code}" --connect-timeout 2 --max-time 5 "${URL}" || true)"
    if [ -n "${code}" ] && [ "${code}" != "000" ]; then
      echo "⏳ waiting for ${URL} (last http=${code})..."
    else
      echo "⏳ waiting for ${URL} (no response yet)..."
    fi
    last_log="$now"
  fi

  if (( now - start > TIMEOUT )); then
    echo "⏰ Timed out waiting for ${URL}"
    exit 1
  fi

  sleep 1
done
