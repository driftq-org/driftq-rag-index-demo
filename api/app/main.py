from __future__ import annotations

import asyncio
import logging
import uuid

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from .driftq_client import DriftQClient
from .models import BuildRequest, ReplayRequest, RollbackRequest, EnqueueResponse
from .qdrant_http import QdrantHTTP
from .storage import (
    get_run_state,
    init_run_state,
    next_version,
    get_history,
    append_log,
    set_run_state,
)

logger = logging.getLogger("demo.api")
logging.basicConfig(level=logging.INFO)

BUILD_TOPIC = "demo.rag.build"
CONTROL_TOPIC = "demo.rag.control"

app = FastAPI(title="driftq-rag-index-demo", version="0.1.0")


async def _startup_retry(fn, seconds: int = 60):
    start = asyncio.get_event_loop().time()
    last_err: Exception | None = None
    while True:
        try:
            return await fn()
        except Exception as e:
            last_err = e
            if asyncio.get_event_loop().time() - start > seconds:
                raise last_err
            await asyncio.sleep(1)


@app.on_event("startup")
async def startup():
    driftq = DriftQClient()

    async def ensure():
        r1 = await driftq.ensure_topic(BUILD_TOPIC, partitions=1)
        r2 = await driftq.ensure_topic(CONTROL_TOPIC, partitions=1)
        logger.info("ensured topics: build=%s control=%s", r1.get("status", "ok"), r2.get("status", "ok"))

    await _startup_retry(ensure, seconds=60)


@app.get("/healthz")
async def healthz():
    driftq = DriftQClient()
    q = QdrantHTTP()

    driftq_ok = await driftq.healthz()
    q_ok = False
    try:
        q_ok = await q.readyz()
    except Exception as e:
        logger.warning("qdrant readyz error: %s", e)
        q_ok = False

    if driftq_ok and q_ok:
        return {"ok": True}

    return JSONResponse(
        status_code=503,
        content={"ok": False, "driftq_ok": driftq_ok, "qdrant_ok": q_ok},
    )


@app.post("/demo/build", response_model=EnqueueResponse)
async def demo_build(req: BuildRequest):
    run_id = str(uuid.uuid4())
    index = req.index
    dataset = req.dataset
    version = next_version(index)

    try:
        init_run_state(
            run_id=run_id,
            index=index,
            dataset=dataset,
            version=version,
            fail_step=req.fail_step,
            fail_mode=req.fail_mode,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "state_init_failed", "message": str(e)})

    driftq = DriftQClient()
    payload = {
        "run_id": run_id,
        "index": index,
        "dataset": dataset,
        "version": version,
        "fail_step": req.fail_step,
        "fail_mode": req.fail_mode,
    }

    try:
        await driftq.produce(topic=BUILD_TOPIC, value=payload, idempotency_key=run_id)
    except Exception as e:
        # mark run failed so UI/demo can show it
        try:
            append_log(run_id, f"ERROR: enqueue failed: {e}")
            st = get_run_state(run_id)
            st["status"] = "FAILED"
            st["errors"] = st.get("errors", [])
            st["errors"].append({"step": "enqueue", "error": str(e)})
            set_run_state(run_id, st)
        except Exception:
            pass

        raise HTTPException(status_code=502, detail={"error": "enqueue_failed", "message": str(e)})

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
