#!/usr/bin/env bash
set -euo pipefail

API_URL="http://localhost:8000"

# Load .env if present (optional)
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

DEMO_INDEX_NAME="${DEMO_INDEX_NAME:-demo}"
DEMO_DATASET="${DEMO_DATASET:-sample}"
DEMO_FAIL_STEP="${DEMO_FAIL_STEP:-embed}"
DEMO_FAIL_MODE="${DEMO_FAIL_MODE:-once}"

echo "🚀 Bringing services up..."
docker compose up -d --build

echo "⏳ Waiting for API..."
bash scripts/wait_for.sh "${API_URL}/healthz" 60

echo "✅ API is up."

echo "🧪 1) Start a build that will fail at step='${DEMO_FAIL_STEP}' (mode='${DEMO_FAIL_MODE}')..."
RUN_JSON=$(curl -sS -X POST "${API_URL}/demo/build" \
  -H 'content-type: application/json' \
  -d "{"index":"${DEMO_INDEX_NAME}","dataset":"${DEMO_DATASET}","fail_step":"${DEMO_FAIL_STEP}","fail_mode":"${DEMO_FAIL_MODE}"}")

RUN_ID=$(python - <<'PY'
import json,sys
print(json.loads(sys.stdin.read())["run_id"])
PY
<<< "${RUN_JSON}")

echo "   run_id=${RUN_ID}"

echo "⏳ Waiting for run to reach FAILED (expected)..."
bash scripts/poll_status.sh "${API_URL}" "${RUN_ID}" "FAILED" 120 || true

STATUS=$(curl -sS "${API_URL}/demo/status/${RUN_ID}" | python -c "import json,sys; print(json.load(sys.stdin).get('status'))")
echo "   status=${STATUS}"

if [ "${STATUS}" != "FAILED" ]; then
  echo "⚠️  Expected FAILED but got '${STATUS}'. Showing status:"
  curl -sS "${API_URL}/demo/status/${RUN_ID}" | python -m json.tool || true
else
  echo "✅ Run failed as expected."
fi

echo "🔁 2) Replay the same run starting at '${DEMO_FAIL_STEP}' (should succeed)..."
curl -sS -X POST "${API_URL}/demo/replay" \
  -H 'content-type: application/json' \
  -d "{"run_id":"${RUN_ID}","from_step":"${DEMO_FAIL_STEP}","fail_step":"none","fail_mode":"never"}" \
  | python -m json.tool

echo "⏳ Waiting for run to reach SUCCEEDED..."
bash scripts/poll_status.sh "${API_URL}" "${RUN_ID}" "SUCCEEDED" 180

echo "✅ Replay succeeded."

echo "🆕 3) Build v2 successfully and promote it..."
RUN2_JSON=$(curl -sS -X POST "${API_URL}/demo/build" \
  -H 'content-type: application/json' \
  -d "{"index":"${DEMO_INDEX_NAME}","dataset":"${DEMO_DATASET}","fail_step":"none","fail_mode":"never"}")

RUN2_ID=$(python - <<'PY'
import json,sys
print(json.loads(sys.stdin.read())["run_id"])
PY
<<< "${RUN2_JSON}")

echo "   run_id=${RUN2_ID}"

bash scripts/poll_status.sh "${API_URL}" "${RUN2_ID}" "SUCCEEDED" 240

echo "✅ v2 build succeeded."

echo "↩️  4) Rollback active alias by 1 version..."
curl -sS -X POST "${API_URL}/demo/rollback" \
  -H 'content-type: application/json' \
  -d "{"index":"${DEMO_INDEX_NAME}","steps":1}" \
  | python -m json.tool

echo "⏳ Waiting for rollback to apply..."
sleep 2

echo "📌 Current index state:"
curl -sS "${API_URL}/demo/index/${DEMO_INDEX_NAME}" | python -m json.tool

echo "🎉 Demo complete."
