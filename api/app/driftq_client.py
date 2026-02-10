from __future__ import annotations

import json
import logging
import os
from typing import Any, AsyncIterator, Dict, Optional

import httpx

logger = logging.getLogger(__name__)


class DriftQClient:
    """
    Minimal DriftQ HTTP client used by:
      - API startup (ensure_topic)
      - API healthz (healthz)
      - Worker (ensure_topic, consume_stream, ack/nack)
    """

    def __init__(self, base_url: Optional[str] = None) -> None:
        # Expect DRIFTQ_HTTP_URL like: http://driftq:8080/v1
        self.base_url = (base_url or os.getenv("DRIFTQ_HTTP_URL", "http://driftq:8080/v1")).rstrip("/")
        self.timeout = httpx.Timeout(10.0, connect=5.0)

    @staticmethod
    def _is_2xx(code: int) -> bool:
        return 200 <= code < 300

    async def healthz(self) -> bool:
        url = f"{self.base_url}/healthz"
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                r = await client.get(url)
            ok = self._is_2xx(r.status_code)
            if not ok:
                logger.warning("driftq healthz unhealthy: %s %s", r.status_code, (r.text or "")[:500])
            return ok
        except Exception as e:
            logger.warning("driftq healthz error: %s", e)
            return False

    async def ensure_topic(self, topic: str, partitions: int = 1) -> Dict[str, Any]:
        """
        Create topic if missing.

        DriftQ may accept different field names depending on version:
          - {"topic": "..."}
          - {"name": "..."}
        DriftQ may return:
          - 201 Created (topic created)
          - 409 Conflict (already exists)
        All of those should be treated as success.
        """
        url = f"{self.base_url}/topics"

        payloads = [
            {"topic": topic, "partitions": partitions},
            {"name": topic, "partitions": partitions},
        ]

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            last: Optional[httpx.Response] = None
            for p in payloads:
                r = await client.post(url, json=p)
                last = r

                if r.status_code in (200, 201, 204, 409):
                    if not r.content:
                        return {"status": "ok"}
                    try:
                        return r.json()
                    except Exception:
                        return {"status": "ok", "raw": r.text}

                # Bad payload shape → try the fallback
                if r.status_code in (400, 422):
                    continue

                raise RuntimeError(f"topics create failed: {r.status_code} {r.text}")

        assert last is not None
        raise RuntimeError(f"topics create failed: {last.status_code} {last.text}")

    async def produce(
        self,
        *,
        topic: str,
        value: Dict[str, Any],
        idempotency_key: Optional[str] = None,
        envelope: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        POST /produce

        DriftQ server variants differ on where idempotency_key lives.
        We'll try:
          1) top-level idempotency_key
          2) nested envelope.idempotency_key
        """
        url = f"{self.base_url}/produce"

        body1: Dict[str, Any] = {"topic": topic, "value": value}
        if envelope:
            body1["envelope"] = dict(envelope)
        if idempotency_key:
            # attempt 1: top-level
            body1["idempotency_key"] = idempotency_key

        body2: Dict[str, Any] = {"topic": topic, "value": value}
        env2: Dict[str, Any] = {}
        if envelope:
            env2.update(envelope)
        if idempotency_key:
            env2["idempotency_key"] = idempotency_key
        if env2:
            body2["envelope"] = env2

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            # Try body1
            r = await client.post(url, json=body1)
            if self._is_2xx(r.status_code):
                return self._safe_json(r)

            # If server rejects the shape, try body2
            if r.status_code in (400, 422):
                r2 = await client.post(url, json=body2)
                if self._is_2xx(r2.status_code):
                    return self._safe_json(r2)
                raise RuntimeError(f"produce failed: {r2.status_code} {r2.text}")

            raise RuntimeError(f"produce failed: {r.status_code} {r.text}")

    async def consume_stream(self, *, topic: str, group: str, lease_ms: int = 30_000) -> AsyncIterator[Dict[str, Any]]:
        """
        Stream messages as NDJSON from DriftQ.
        """
        url = f"{self.base_url}/consume"
        params = {"topic": topic, "group": group, "lease_ms": str(lease_ms)}

        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("GET", url, params=params) as r:
                if not self._is_2xx(r.status_code):
                    body = (await r.aread()).decode("utf-8", errors="replace")
                    raise RuntimeError(f"consume failed: {r.status_code} {body}")

                async for line in r.aiter_lines():
                    line = (line or "").strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except Exception:
                        logger.warning("bad json line from consume: %r", line[:500])

    async def ack(self, *, topic: str, group: str, msg: Dict[str, Any]) -> None:
        """
        Newer DriftQ acks use lease_id. Some older variants used owner/partition/offset.
        We'll try lease_id first, then fallback.
        """
        url = f"{self.base_url}/ack"

        payload1 = {"topic": topic, "group": group, "lease_id": msg.get("lease_id")}
        payload2 = {
            "topic": topic,
            "group": group,
            "owner": msg.get("owner"),
            "partition": msg.get("partition"),
            "offset": msg.get("offset"),
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.post(url, json=payload1)
            if r.status_code in (200, 204):
                return
            if r.status_code in (400, 422):
                r2 = await client.post(url, json=payload2)
                if r2.status_code in (200, 204):
                    return
                raise RuntimeError(f"ack failed: {r2.status_code} {r2.text}")
            if r.status_code == 409:
                # Not owner / lease expired — not fatal for demo
                logger.warning("ack not-owner (409); ignoring")
                return
            raise RuntimeError(f"ack failed: {r.status_code} {r.text}")

    async def nack(self, *, topic: str, group: str, msg: Dict[str, Any]) -> None:
        url = f"{self.base_url}/nack"

        payload1 = {"topic": topic, "group": group, "lease_id": msg.get("lease_id")}
        payload2 = {
            "topic": topic,
            "group": group,
            "owner": msg.get("owner"),
            "partition": msg.get("partition"),
            "offset": msg.get("offset"),
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.post(url, json=payload1)
            if r.status_code in (200, 204):
                return
            if r.status_code in (400, 422):
                r2 = await client.post(url, json=payload2)
                if r2.status_code in (200, 204):
                    return
                raise RuntimeError(f"nack failed: {r2.status_code} {r2.text}")
            if r.status_code == 409:
                logger.warning("nack not-owner (409); ignoring")
                return
            raise RuntimeError(f"nack failed: {r.status_code} {r.text}")

    def extract_value(self, msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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

    @staticmethod
    def _safe_json(r: httpx.Response) -> Dict[str, Any]:
        if not r.content:
            return {"status": "ok"}
        try:
            return r.json()
        except Exception:
            return {"status": "ok", "raw": r.text}
