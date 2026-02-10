from __future__ import annotations

import json
import os
from typing import Any, AsyncIterator, Dict, Optional

import httpx

class DriftQClient:
    """
    Minimal DriftQ-Core HTTP client for the demo.

    Assumes DriftQ-Core v1 API style:
      POST /v1/produce
      GET  /v1/consume   (NDJSON stream)
      POST /v1/ack
      POST /v1/nack
      GET/POST /v1/topics
    """

    def __init__(self, base_url: Optional[str] = None) -> None:
        self.base_url = (base_url or os.getenv("DRIFTQ_HTTP_URL") or "http://localhost:8080/v1").rstrip("/")

    async def healthz(self) -> bool:
        async with httpx.AsyncClient(timeout=2.0) as c:
            r = await c.get(f"{self.base_url}/healthz")
            return r.status_code == 200

    async def ensure_topic(self, topic: str, partitions: int = 1) -> None:
        async with httpx.AsyncClient(timeout=5.0) as c:
            r = await c.post(f"{self.base_url}/topics", json={"topic": topic, "partitions": partitions})
            # If topic exists, DriftQ may return 409 or 200 depending on implementation.
            if r.status_code in (200, 201, 204, 409):
                return
            raise RuntimeError(f"topics create failed: {r.status_code} {r.text}")

    async def produce(self, *, topic: str, value: Any, idempotency_key: Optional[str] = None) -> None:
        payload = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
        body: Dict[str, Any] = {"topic": topic, "value": payload}
        if idempotency_key:
            body["envelope"] = {"idempotency_key": idempotency_key}
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.post(f"{self.base_url}/produce", json=body)
            if r.status_code in (200, 201, 204):
                return
            raise RuntimeError(f"produce failed: {r.status_code} {r.text}")

    async def consume_stream(self, *, topic: str, group: str, lease_ms: int = 30000) -> AsyncIterator[Dict[str, Any]]:
        params = {"topic": topic, "group": group, "lease_ms": str(lease_ms)}
        async with httpx.AsyncClient(timeout=None) as c:
            async with c.stream("GET", f"{self.base_url}/consume", params=params) as r:
                r.raise_for_status()
                async for line in r.aiter_lines():
                    if not line:
                        continue
                    yield json.loads(line)

    @staticmethod
    def extract_value(msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        v = msg.get("value")
        if isinstance(v, dict):
            return v
        if isinstance(v, str):
            s = v.strip()
            if s.startswith("{") or s.startswith("["):
                try:
                    decoded = json.loads(s)
                    if isinstance(decoded, dict):
                        return decoded
                    return {"value": decoded}
                except Exception:
                    return None
        return None

    def _ack_body(self, *, topic: str, group: str, msg: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "topic": topic,
            "group": group,
            "owner": msg.get("owner"),
            "partition": msg.get("partition"),
            "offset": msg.get("offset"),
        }

    async def ack(self, *, topic: str, group: str, msg: Dict[str, Any]) -> None:
        body = self._ack_body(topic=topic, group=group, msg=msg)
        async with httpx.AsyncClient(timeout=5.0) as c:
            r = await c.post(f"{self.base_url}/ack", json=body)
            if r.status_code in (200, 204):
                return
            raise RuntimeError(f"ack failed: {r.status_code} {r.text}")

    async def nack(self, *, topic: str, group: str, msg: Dict[str, Any]) -> None:
        body = self._ack_body(topic=topic, group=group, msg=msg)
        async with httpx.AsyncClient(timeout=5.0) as c:
            r = await c.post(f"{self.base_url}/nack", json=body)
            if r.status_code in (200, 204):
                return
            raise RuntimeError(f"nack failed: {r.status_code} {r.text}")
