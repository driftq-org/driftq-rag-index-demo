#!/usr/bin/env bash
set -euo pipefail

# Load .env if present (optional)
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

API_BASE_URL="${API_BASE_URL:-http://localhost:8000}"

DEMO_INDEX_NAME="${DEMO_INDEX_NAME:-demo}"
DEMO_DATASET="${DEMO_DATASET:-sample}"
DEMO_FAIL_STEP="${DEMO_FAIL_STEP:-embed}"
DEMO_FAIL_MODE="${DEMO_FAIL_MODE:-once}"

# Prefer python, fall back to python3.
PY_BIN="${PY_BIN:-python}"
if ! command -v "${PY_BIN}" >/dev/null 2>&1; then
  PY_BIN="python3"
fi
command -v "${PY_BIN}" >/dev/null 2>&1 || {
  echo "❌ Neither 'python' nor 'python3' found. Demo runner needs Python to parse JSON."
  exit 127
}

# If docker is available, bring services up (host mode).
# In the demo-runner container, docker won't exist, so this is skipped.
if command -v docker >/dev/null 2>&1; then
  echo "🚀 Bringing services up..."
  docker compose up -d --build
else
  echo "ℹ️  Docker not found (likely running inside demo container). Skipping 'docker compose up'."
fi

echo "⏳ Waiting for API..."
bash scripts/wait_for.sh "${API_BASE_URL}/healthz" 60
echo "✅ API is up."

echo "🧪 1) Start a build that will fail at step='${DEMO_FAIL_STEP}' (mode='${DEMO_FAIL_MODE}')..."
RUN_JSON="$(curl -sS -X POST "${API_BASE_URL}/demo/build" \
  -H 'content-type: application/json' \
  -d "$("${PY_BIN}" - <<PY
import json
print(json.dumps({
  "index": "${DEMO_INDEX_NAME}",
  "dataset": "${DEMO_DATASET}",
  "fail_step": "${DEMO_FAIL_STEP}",
  "fail_mode": "${DEMO_FAIL_MODE}",
}))
PY
)")"

# ✅ FIX: don't mix heredoc + here-string; pipe JSON into python -c
RUN_ID="$(printf '%s' "${RUN_JSON}" | "${PY_BIN}" -c 'import json,sys; print(json.load(sys.stdin)["run_id"])')"

echo "   run_id=${RUN_ID}"

echo "⏳ Waiting for run to reach FAILED (expected)..."
bash scripts/poll_status.sh "${API_BASE_URL}" "${RUN_ID}" "FAILED" 120 || true

STATUS="$(curl -sS "${API_BASE_URL}/demo/status/${RUN_ID}" | "${PY_BIN}" -c 'import json,sys; print(json.load(sys.stdin).get("status"))')"
echo "   status=${STATUS}"

if [ "${STATUS}" != "FAILED" ]; then
  echo "⚠️  Expected FAILED but got '${STATUS}'. Showing status:"
  curl -sS "${API_BASE_URL}/demo/status/${RUN_ID}" | "${PY_BIN}" -m json.tool || true
else
  echo "✅ Run failed as expected."
fi

echo "🔁 2) Replay the same run starting at '${DEMO_FAIL_STEP}' (should succeed)..."
curl -sS -X POST "${API_BASE_URL}/demo/replay" \
  -H 'content-type: application/json' \
  -d "$("${PY_BIN}" - <<PY
import json
print(json.dumps({
  "run_id": "${RUN_ID}",
  "from_step": "${DEMO_FAIL_STEP}",
  "fail_step": "none",
  "fail_mode": "never",
}))
PY
)" | "${PY_BIN}" -m json.tool

echo "⏳ Waiting for run to reach SUCCEEDED..."
bash scripts/poll_status.sh "${API_BASE_URL}" "${RUN_ID}" "SUCCEEDED" 180
echo "✅ Replay succeeded."

echo "🆕 3) Build v2 successfully and promote it..."
RUN2_JSON="$(curl -sS -X POST "${API_BASE_URL}/demo/build" \
  -H 'content-type: application/json' \
  -d "$("${PY_BIN}" - <<PY
import json
print(json.dumps({
  "index": "${DEMO_INDEX_NAME}",
  "dataset": "${DEMO_DATASET}",
  "fail_step": "none",
  "fail_mode": "never",
}))
PY
)")"

# ✅ FIX here too
RUN2_ID="$(printf '%s' "${RUN2_JSON}" | "${PY_BIN}" -c 'import json,sys; print(json.load(sys.stdin)["run_id"])')"

echo "   run_id=${RUN2_ID}"

bash scripts/poll_status.sh "${API_BASE_URL}" "${RUN2_ID}" "SUCCEEDED" 240
echo "✅ v2 build succeeded."

echo "↩️  4) Rollback active alias by 1 version..."
curl -sS -X POST "${API_BASE_URL}/demo/rollback" \
  -H 'content-type: application/json' \
  -d "$("${PY_BIN}" - <<PY
import json
print(json.dumps({
  "index": "${DEMO_INDEX_NAME}",
  "steps": 1,
}))
PY
)" | "${PY_BIN}" -m json.tool

echo "⏳ Waiting for rollback to apply..."
sleep 2

echo "📌 Current index state:"
curl -sS "${API_BASE_URL}/demo/index/${DEMO_INDEX_NAME}" | "${PY_BIN}" -m json.tool

echo "🎉 Demo complete."
