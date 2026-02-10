from __future__ import annotations

import asyncio
import uuid
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from .driftq_client import DriftQClient
from .models import BuildRequest, ReplayRequest, RollbackRequest, EnqueueResponse
from .qdrant_http import QdrantHTTP
from .storage import get_run_state, init_run_state, next_version, get_history

BUILD_TOPIC = "demo.rag.build"
CONTROL_TOPIC = "demo.rag.control"

app = FastAPI(title="driftq-rag-index-demo", version="0.1.0")

@app.get("/healthz")
async def healthz():
    driftq = DriftQClient()
    q = QdrantHTTP()
    try:
        driftq_ok = await driftq.healthz()
    except Exception:
        driftq_ok = False
    try:
        q_ok = await q.ready()
    except Exception:
        q_ok = False

    ok = driftq_ok and q_ok
    if ok:
        return {"ok": True}
    return JSONResponse({"ok": False, "driftq_ok": driftq_ok, "qdrant_ok": q_ok}, status_code=503)

async def _startup_retry(fn, *, seconds: int = 60) -> None:
    for _ in range(seconds):
        try:
            await fn()
            return
        except Exception:
            await asyncio.sleep(1)
    # last try (raise)
    await fn()

@app.on_event("startup")
async def startup():
    driftq = DriftQClient()

    async def ensure():
        await driftq.ensure_topic(BUILD_TOPIC, partitions=1)
        await driftq.ensure_topic(CONTROL_TOPIC, partitions=1)

    # DriftQ may not be ready the instant the container starts.
    await _startup_retry(ensure, seconds=60)

@app.post("/demo/build", response_model=EnqueueResponse)
async def demo_build(req: BuildRequest):
    run_id = str(uuid.uuid4())
    index = req.index
    dataset = req.dataset
    version = next_version(index)

    init_run_state(
        run_id=run_id,
        index=index,
        dataset=dataset,
        version=version,
        fail_step=req.fail_step,
        fail_mode=req.fail_mode,
    )

    driftq = DriftQClient()
    await driftq.produce(
        topic=BUILD_TOPIC,
        value={
            "type": "build",
            "run_id": run_id,
            "index": index,
            "dataset": dataset,
            "version": version,
            "fail_step": req.fail_step,
            "fail_mode": req.fail_mode,
        },
        idempotency_key=run_id,
    )
    return EnqueueResponse(run_id=run_id, queued=True, topic=BUILD_TOPIC)

@app.post("/demo/replay")
async def demo_replay(req: ReplayRequest):
    state = get_run_state(req.run_id)
    if state.get("status") == "UNKNOWN":
        raise HTTPException(status_code=404, detail="unknown run_id")

    driftq = DriftQClient()
    await driftq.produce(
        topic=BUILD_TOPIC,
        value={
            "type": "replay",
            "run_id": req.run_id,
            "index": state.get("index", "demo"),
            "dataset": state.get("dataset", "sample"),
            "version": state.get("version", 1),
            "from_step": req.from_step,
            "fail_step": req.fail_step,
            "fail_mode": req.fail_mode,
        },
        idempotency_key=f"{req.run_id}:replay:{req.from_step}",
    )
    return {"ok": True, "run_id": req.run_id, "from_step": req.from_step}

@app.post("/demo/rollback")
async def demo_rollback(req: RollbackRequest):
    driftq = DriftQClient()
    await driftq.produce(
        topic=CONTROL_TOPIC,
        value={
            "type": "rollback",
            "index": req.index,
            "steps": req.steps,
            "to_version": req.to_version,
        },
        idempotency_key=f"{req.index}:rollback:{req.steps}:{req.to_version}",
    )
    return {"ok": True, "queued": True, "topic": CONTROL_TOPIC}

@app.get("/demo/status/{run_id}")
async def demo_status(run_id: str):
    return JSONResponse(get_run_state(run_id))

@app.get("/demo/index/{index}")
async def demo_index(index: str):
    q = QdrantHTTP()
    alias = f"demo_{index}_active"
    target = None
    try:
        target = await q.get_alias_target(alias)
    except Exception:
        target = None
    hist = get_history(index)
    return {"index": index, "alias": alias, "alias_target": target, "history": hist}
