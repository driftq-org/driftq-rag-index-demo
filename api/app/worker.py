from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any, Dict

from .driftq_client import DriftQClient
from .models import FailMode
from .pipeline import discover_step, chunk_step, embed_step, should_fail
from .qdrant_http import QdrantHTTP
from .storage import (
    append_log,
    get_run_state,
    init_run_state,
    next_version,
    previous_version,
    record_version,
    run_dir,
    set_active,
    set_run_state,
    set_history,
    get_history,
)

BUILD_TOPIC = "demo.rag.build"
CONTROL_TOPIC = "demo.rag.control"
WORKER_GROUP = "demo-rag-worker"

def collection_name(index: str, version: int) -> str:
    return f"demo_{index}_v{version}"

def alias_name(index: str) -> str:
    return f"demo_{index}_active"

async def handle_build(q: QdrantHTTP, msg_value: Dict[str, Any]) -> None:
    run_id = msg_value["run_id"]
    index = msg_value.get("index", "demo")
    dataset = msg_value.get("dataset", "sample")
    version = int(msg_value.get("version") or next_version(index))
    from_step = msg_value.get("from_step")  # optional
    fail_step = msg_value.get("fail_step", "none")
    fail_mode = msg_value.get("fail_mode", "never")
    dim = int(os.getenv("EMBED_DIM", "16"))

    # initialize state if not exists
    state = get_run_state(run_id)
    if state.get("status") in ("UNKNOWN", "QUEUED"):
        init_run_state(run_id=run_id, index=index, dataset=dataset, version=version, fail_step=fail_step, fail_mode=fail_mode)
        state = get_run_state(run_id)

    state["status"] = "RUNNING"
    set_run_state(run_id, state)
    append_log(run_id, f"Starting build (from_step={from_step or 'discover'})")

    # cache markers
    fail_marker = run_dir(run_id) / f"fail_{fail_step}.marker"

    # Step helper
    async def step(name: str, fn):
        st = get_run_state(run_id)
        st["steps"][name]["status"] = "RUNNING"
        set_run_state(run_id, st)
        try:
            path = fn()
            st = get_run_state(run_id)
            st["steps"][name]["status"] = "SUCCEEDED"
            st["artifacts"][name] = str(path)
            set_run_state(run_id, st)
            return path
        except Exception as e:
            st = get_run_state(run_id)
            st["steps"][name]["status"] = "FAILED"
            st["errors"].append({"step": name, "error": str(e)})
            st["status"] = "FAILED"
            set_run_state(run_id, st)
            append_log(run_id, f"{name}: FAILED: {e}")
            raise

    # Resume logic: if artifact exists, skip
    def artifact_exists(step_name: str) -> bool:
        st = get_run_state(run_id)
        p = st.get("artifacts", {}).get(step_name)
        return bool(p) and os.path.exists(p)

    # Determine starting step
    start = from_step or "discover"
    order = ["discover", "chunk", "embed", "upsert", "promote", "smoketest"]
    start_idx = order.index(start) if start in order else 0

    try:
        # discover
        if start_idx <= 0 and not artifact_exists("discover"):
            await step("discover", lambda: discover_step(run_id, dataset))
        else:
            append_log(run_id, "discover: using cached output")

        # chunk
        if start_idx <= 1 and not artifact_exists("chunk"):
            await step("chunk", lambda: chunk_step(run_id))
        else:
            append_log(run_id, "chunk: using cached output")

        # fail injection before embed
        if should_fail(fail_step=fail_step, fail_mode=fail_mode, step="embed", marker_path=fail_marker):
            raise RuntimeError("Injected failure at embed (demo)")

        # embed
        if start_idx <= 2 and not artifact_exists("embed"):
            await step("embed", lambda: embed_step(run_id, dim))
        else:
            append_log(run_id, "embed: using cached output")

        # upsert to Qdrant (always re-run if start <= upsert)
        if start_idx <= 3:
            st = get_run_state(run_id)
            st["steps"]["upsert"]["status"] = "RUNNING"
            set_run_state(run_id, st)

            coll = collection_name(index, version)
            await q.create_collection(coll, vector_size=dim)

            # load embeddings
            embeds_path = run_dir(run_id) / "embeddings.json"
            embeds = __import__("json").loads(embeds_path.read_text(encoding="utf-8"))
            points = []
            for e in embeds:
                # stable point ID
                pid = int(__import__("hashlib").sha256(e["chunk_id"].encode("utf-8")).hexdigest()[:16], 16)
                points.append({
                    "id": pid,
                    "vector": e["vector"],
                    "payload": {"chunk_id": e["chunk_id"], "doc_id": e["doc_id"], "text": e["text"]},
                })
            # batch
            B=128
            for i in range(0, len(points), B):
                await q.upsert_points(coll, points[i:i+B])

            st = get_run_state(run_id)
            st["steps"]["upsert"]["status"] = "SUCCEEDED"
            st["artifacts"]["upsert"] = coll
            set_run_state(run_id, st)
            append_log(run_id, f"upsert: wrote {len(points)} points to {coll}")
        else:
            append_log(run_id, "upsert: skipped (replay from later step)")

        # promote (alias update)
        if start_idx <= 4:
            st = get_run_state(run_id)
            st["steps"]["promote"]["status"] = "RUNNING"
            set_run_state(run_id, st)

            coll = collection_name(index, version)
            await q.set_alias(alias_name(index), coll)
            record_version(index, version)
            set_active(index, version)

            st = get_run_state(run_id)
            st["steps"]["promote"]["status"] = "SUCCEEDED"
            st["artifacts"]["promote"] = {"alias": alias_name(index), "collection": coll}
            set_run_state(run_id, st)
            append_log(run_id, f"promote: {alias_name(index)} -> {coll}")
        else:
            append_log(run_id, "promote: skipped (replay from later step)")

        # smoketest
        if start_idx <= 5:
            st = get_run_state(run_id)
            st["steps"]["smoketest"]["status"] = "RUNNING"
            set_run_state(run_id, st)

            alias = alias_name(index)
            # basic query
            queries = ["distributed systems retries", "rag index versioning", "api security auth"]
            results = []
            from .pipeline import fake_embed
            for qtxt in queries:
                vec = fake_embed(qtxt, dim)
                hits = await q.search(alias, vec, limit=3)
                results.append({"query": qtxt, "hits": hits})
            out = run_dir(run_id) / "smoketest.json"
            out.write_text(__import__("json").dumps(results, indent=2), encoding="utf-8")

            st = get_run_state(run_id)
            st["steps"]["smoketest"]["status"] = "SUCCEEDED"
            st["artifacts"]["smoketest"] = str(out)
            st["status"] = "SUCCEEDED"
            set_run_state(run_id, st)
            append_log(run_id, "smoketest: SUCCEEDED")
        else:
            st = get_run_state(run_id)
            st["status"] = "SUCCEEDED"
            set_run_state(run_id, st)
            append_log(run_id, "smoketest: skipped; marking run SUCCEEDED")

    except Exception as e:
        # if it's injected failure, mark failed but keep artifacts for replay
        st = get_run_state(run_id)
        if st.get("status") != "FAILED":
            st["status"] = "FAILED"
            st["errors"].append({"step": "runtime", "error": str(e)})
            set_run_state(run_id, st)
        append_log(run_id, f"Run FAILED: {e}")
        return

async def handle_rollback(q: QdrantHTTP, msg_value: Dict[str, Any]) -> Dict[str, Any]:
    index = msg_value.get("index", "demo")
    steps = int(msg_value.get("steps") or 1)
    to_version = msg_value.get("to_version")
    hist = get_history(index)
    active = hist.get("active")
    if active is None:
        return {"ok": False, "error": "no active version"}
    if to_version is None:
        to_version = previous_version(index, steps=steps)
    if to_version is None:
        return {"ok": False, "error": "no previous version available"}
    coll = collection_name(index, int(to_version))
    await q.set_alias(alias_name(index), coll)
    set_active(index, int(to_version))
    return {"ok": True, "index": index, "active": int(to_version), "collection": coll}

async def consume_loop(topic: str, handler):
    driftq = DriftQClient()
    q = QdrantHTTP()

    await driftq.ensure_topic(topic, partitions=1)

    while True:
        try:
            async for msg in driftq.consume_stream(topic=topic, group=WORKER_GROUP, lease_ms=30000):
                val = driftq.extract_value(msg) or {}
                try:
                    if topic == BUILD_TOPIC:
                        await handle_build(q, val)
                    else:
                        # control
                        await handler(q, val)
                    await driftq.ack(topic=topic, group=WORKER_GROUP, msg=msg)
                except Exception as e:
                    # nack so DriftQ can redeliver (helps validate core redelivery)
                    await driftq.nack(topic=topic, group=WORKER_GROUP, msg=msg)
        except Exception:
            await asyncio.sleep(1.0)

async def main():
    print("worker starting...")
    # consume build + control in parallel
    await asyncio.gather(
        consume_loop(BUILD_TOPIC, handler=handle_rollback),
        consume_loop(CONTROL_TOPIC, handler=handle_rollback),
    )

if __name__ == "__main__":
    asyncio.run(main())
