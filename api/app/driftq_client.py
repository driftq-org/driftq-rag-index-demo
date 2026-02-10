from __future__ import annotations

import json
import logging
import os
import uuid
from typing import Any, AsyncIterator, Dict, Optional, Union

import httpx

logger = logging.getLogger(__name__)

JsonLike = Union[Dict[str, Any], list, str, int, float, bool, None]

class DriftQClient:
    """
    Minimal DriftQ HTTP client used by:
      - API startup (ensure_topic)
      - API healthz (healthz)
      - Worker (ensure_topic, consume_stream, ack/nack)
    """

    def __init__(self, base_url: Optional[str] = None, *, owner: Optional[str] = None) -> None:
        # Expect DRIFTQ_HTTP_URL like: http://driftq:8080/v1
        self.base_url = (base_url or os.getenv("DRIFTQ_HTTP_URL", "http://driftq:8080/v1")).rstrip("/")
        self.timeout = httpx.Timeout(10.0)
        # DriftQ v1 consume requires an "owner" (lease owner). Keep it stable per process.
        self.owner = (
            owner
            or os.getenv("DRIFTQ_OWNER")
            or os.getenv("HOSTNAME")
            or uuid.uuid4().hex[:12]
        )

    def _is_ok(self, status_code: int) -> bool:
        return 200 <= status_code < 300

    async def healthz(self) -> bool:
        url = f"{self.base_url}/healthz"
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                r = await client.get(url)
            ok = self._is_ok(r.status_code)
            if not ok:
                logger.warning("driftq healthz unhealthy: %s %s", r.status_code, r.text[:500])
            return ok
        except Exception as e:
            logger.warning("driftq healthz error: %s", e)
            return False

    async def ensure_topic(self, topic: str, partitions: int = 1) -> Dict[str, Any]:
        """
        Create topic if missing.

        Your DriftQ container is accepting one payload shape and rejecting another (400 then 201).
        So: try {"topic": "..."} first, then fallback to {"name": "..."}.
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

            # success / already exists
            if r.status_code in (200, 201, 204, 409):
                if not r.content:
                    return {"status": "ok"}
                try:
                    return r.json()
                except Exception:
                    return {"status": "ok", "raw": r.text}

            # try next payload shape if it's a payload validation problem
            if r.status_code in (400, 422):
                continue

            raise RuntimeError(f"topics create failed: {r.status_code} {r.text}")

        assert last is not None
        raise RuntimeError(f"topics create failed: {last.status_code} {last.text}")

    async def produce(
        self,
        *,
        topic: str,
        value: JsonLike,
        idempotency_key: Optional[str] = None,
        tenant_id: Optional[str] = None,
        envelope: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        IMPORTANT (based on your error): DriftQ expects ProduceRequest.value to be a STRING.
        So we JSON-encode dict values.
        """
        url = f"{self.base_url}/produce"
        value_str = json.dumps(value, separators=(",", ":"), ensure_ascii=False)

        # We attempt a couple request shapes because DriftQ servers vary.
        attempts: list[Dict[str, Any]] = []

        # Shape A: top-level fields
        payload_a: Dict[str, Any] = {"topic": topic, "value": value_str}
        if idempotency_key:
            payload_a["idempotency_key"] = idempotency_key
        if tenant_id:
            payload_a["tenant_id"] = tenant_id
            payload_a["tenant"] = tenant_id  # alias
        if envelope:
            payload_a["envelope"] = envelope
        attempts.append(payload_a)

        # Shape B: everything inside envelope (some servers prefer this)
        payload_b: Dict[str, Any] = {"topic": topic, "value": value_str}
        env: Dict[str, Any] = {}
        if envelope:
            env.update(envelope)
        if idempotency_key:
            env["idempotency_key"] = idempotency_key
        if tenant_id:
            env["tenant_id"] = tenant_id
            env["tenant"] = tenant_id
        if env:
            payload_b["envelope"] = env
        attempts.append(payload_b)

        last: Optional[httpx.Response] = None
        for payload in attempts:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                r = await client.post(url, json=payload)
            last = r

            if self._is_ok(r.status_code):
                if not r.content:
                    return {"status": "ok"}
                try:
                    return r.json()
                except Exception:
                    return {"status": "ok", "raw": r.text}

            # If server says our JSON shape is wrong, try the next shape.
            if r.status_code in (400, 422):
                continue

            raise RuntimeError(f"produce failed: {r.status_code} {r.text}")

        assert last is not None
        raise RuntimeError(f"produce failed: {last.status_code} {last.text}")

    async def consume_stream(
        self,
        *,
        topic: str,
        group: str,
        owner: Optional[str] = None,
        lease_ms: int = 30_000,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Stream messages as NDJSON from DriftQ.

        Your DriftQ requires: topic, group, owner
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
                if not self._is_ok(r.status_code):
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
        """
        Your server is NOT lease_id-based (it errors on 'lease_id').
        It appears offset-based: owner + partition + offset.
        We'll support both formats.
        """
        url = f"{self.base_url}/ack"
        await self._ack_like(url, topic=topic, group=group, msg=msg, owner=owner)

    async def nack(self, *, topic: str, group: str, msg: Dict[str, Any], owner: Optional[str] = None) -> None:
        url = f"{self.base_url}/nack"
        await self._ack_like(url, topic=topic, group=group, msg=msg, owner=owner)

    async def _ack_like(self, url: str, *, topic: str, group: str, msg: Dict[str, Any], owner: Optional[str]) -> None:
        # Try lease_id style first (older servers)
        lease_id = msg.get("lease_id") or msg.get("leaseId")
        lease = msg.get("lease")
        if isinstance(lease, dict) and not lease_id:
            lease_id = lease.get("lease_id") or lease.get("leaseId") or lease.get("lease")

        # Newer shapes may tuck lease info in envelope/routing
        for parent_key in ("envelope", "routing"):
            parent = msg.get(parent_key)
            if isinstance(parent, dict) and not lease_id:
                lease_id = parent.get("lease_id") or parent.get("leaseId") or parent.get("lease")
                if not lease_id:
                    embedded = parent.get("lease")
                    if isinstance(embedded, dict):
                        lease_id = (
                            embedded.get("lease_id")
                            or embedded.get("leaseId")
                            or embedded.get("lease")
                        )

        # Try offset style (your current server)
        partition = msg.get("partition")
        offset = msg.get("offset")
        if offset is None:
            offset = msg.get("Offset")  # just in case

        payloads: list[Dict[str, Any]] = []

        if lease_id:
            payloads.append({"topic": topic, "group": group, "lease_id": lease_id})
            payloads.append({"topic": topic, "group": group, "lease": lease_id})

        if partition is not None and offset is not None:
            payloads.append(
                {
                    "topic": topic,
                    "group": group,
                    "owner": owner or self.owner,
                    "partition": partition,
                    "offset": offset,
                }
            )

        if not payloads:
            raise RuntimeError(f"ack/nack: missing lease_id OR (partition+offset) in message keys={list(msg.keys())}")

        last: Optional[httpx.Response] = None
        for payload in payloads:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                r = await client.post(url, json=payload)
            last = r

            if self._is_ok(r.status_code):
                return

            # payload mismatch? try next shape
            if r.status_code in (400, 422):
                continue

            raise RuntimeError(f"ack/nack failed: {r.status_code} {r.text}")

        assert last is not None
        raise RuntimeError(f"ack/nack failed: {last.status_code} {last.text}")

    def extract_value(self, msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Worker helper:
          - server may return value as dict OR as a JSON string
        """
        v = msg.get("value")
        if isinstance(v, dict):
            return v
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                return None
        return None
