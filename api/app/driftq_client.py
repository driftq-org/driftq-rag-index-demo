from __future__ import annotations

import json
import logging
import os
import socket
from typing import Any, AsyncIterator, Dict, Optional, Union

import httpx

logger = logging.getLogger(__name__)


class DriftQClient:
    """
    Minimal DriftQ HTTP client used by:
      - API startup (ensure_topic)
      - API healthz (healthz)
      - Worker (ensure_topic, consume_stream, ack/nack)
    """

    def __init__(self, base_url: Optional[str] = None, owner: Optional[str] = None) -> None:
        # Expect DRIFTQ_HTTP_URL like: http://driftq:8080/v1
        self.base_url = (base_url or os.getenv("DRIFTQ_HTTP_URL", "http://driftq:8080/v1")).rstrip("/")
        self.timeout = httpx.Timeout(10.0)
        # DriftQ consume requires an owner (per your 400 error). Use stable hostname by default.
        self.owner = owner or os.getenv("DRIFTQ_OWNER") or socket.gethostname()

    def _is_healthy_status(self, status_code: int) -> bool:
        return 200 <= status_code < 300

    async def healthz(self) -> bool:
        url = f"{self.base_url}/healthz"
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                r = await client.get(url)
            ok = self._is_healthy_status(r.status_code)
            if not ok:
                logger.warning("driftq healthz unhealthy: %s %s", r.status_code, r.text[:500])
            return ok
        except Exception as e:
            logger.warning("driftq healthz error: %s", e)
            return False

    async def ensure_topic(self, topic: str, partitions: int = 1) -> Dict[str, Any]:
        """
        Create topic if missing.

        Your server clearly accepts {"topic": "..."} (and sometimes rejects {"name": "..."} with 400),
        so we try "topic" first to avoid noisy 400 logs, then fall back to "name".
        """
        url = f"{self.base_url}/topics"

        async def _post(payload: Dict[str, Any]) -> httpx.Response:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                return await client.post(url, json=payload)

        payloads = [
            {"topic": topic, "partitions": partitions},
            {"name": topic, "partitions": partitions},
        ]

        last: Optional[httpx.Response] = None
        for p in payloads:
            r = await _post(p)
            last = r

            # Success OR already-exists
            if r.status_code in (200, 201, 204, 409):
                if not r.content:
                    return {"status": "ok"}
                try:
                    return r.json()
                except Exception:
                    return {"status": "ok", "raw": r.text}

            # bad payload shape, try fallback
            if r.status_code in (400, 422):
                continue

            raise RuntimeError(f"topics create failed: {r.status_code} {r.text}")

        assert last is not None
        raise RuntimeError(f"topics create failed: {last.status_code} {last.text}")

    async def produce(
        self,
        *,
        topic: str,
        value: Union[Dict[str, Any], str],
        envelope: Optional[Dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        IMPORTANT: DriftQ expects `value` to be a STRING (your error confirms this).
        So if caller passes a dict, we JSON-encode it into a string.
        """
        url = f"{self.base_url}/produce"

        if isinstance(value, str):
            value_str = value
        else:
            value_str = json.dumps(value, ensure_ascii=False, separators=(",", ":"))

        env: Dict[str, Any] = {}
        if envelope:
            env.update(envelope)
        if idempotency_key:
            # The server supports envelope idempotency_key; keep it here.
            env["idempotency_key"] = idempotency_key
        if tenant_id:
            env["tenant_id"] = tenant_id

        payload: Dict[str, Any] = {"topic": topic, "value": value_str}
        if env:
            payload["envelope"] = env

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.post(url, json=payload)

        if not self._is_healthy_status(r.status_code):
            raise RuntimeError(f"produce failed: {r.status_code} {r.text}")

        if not r.content:
            return {"status": "ok"}
        try:
            return r.json()
        except Exception:
            return {"status": "ok", "raw": r.text}

    async def consume_stream(
        self,
        *,
        topic: str,
        group: str,
        lease_ms: int = 30_000,
        owner: Optional[str] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Stream messages as NDJSON from DriftQ.
        Your server requires topic, group, and owner.
        """
        url = f"{self.base_url}/consume"
        params = {
            "topic": topic,
            "group": group,
            "owner": owner or self.owner,
            "lease_ms": str(lease_ms),
        }

        async with httpx.AsyncClient(timeout=None) as client:
            async with client.stream("GET", url, params=params) as r:
                if not self._is_healthy_status(r.status_code):
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

    async def ack(self, *, topic: str, group: str, msg: Dict[str, Any], owner: Optional[str] = None) -> None:
        url = f"{self.base_url}/ack"
        payload = {
            "topic": topic,
            "group": group,
            "lease_id": msg.get("lease_id"),
            "owner": owner or self.owner,
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.post(url, json=payload)

        if not self._is_healthy_status(r.status_code):
            raise RuntimeError(f"ack failed: {r.status_code} {r.text}")

    async def nack(self, *, topic: str, group: str, msg: Dict[str, Any], owner: Optional[str] = None) -> None:
        url = f"{self.base_url}/nack"
        payload = {
            "topic": topic,
            "group": group,
            "lease_id": msg.get("lease_id"),
            "owner": owner or self.owner,
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.post(url, json=payload)

        if not self._is_healthy_status(r.status_code):
            raise RuntimeError(f"nack failed: {r.status_code} {r.text}")

    def extract_value(self, msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Worker helper: messages are typically {"value": "...", "lease_id": "...", ...}

        Since DriftQ stores value as STRING, we decode JSON string back into dict.
        """
        v = msg.get("value")
        if isinstance(v, dict):
            return v
        if isinstance(v, str):
            s = v.strip()
            if not s:
                return None
            try:
                parsed = json.loads(s)
                return parsed if isinstance(parsed, dict) else {"_value": parsed}
            except Exception:
                # Not JSON — still return something so handler can log/debug.
                return {"_raw": v}
        return None
