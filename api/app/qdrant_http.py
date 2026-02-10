from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

import httpx

class QdrantHTTP:
    def __init__(self, base_url: Optional[str] = None) -> None:
        self.base_url = (base_url or os.getenv("QDRANT_URL") or "http://localhost:6333").rstrip("/")

    async def ready(self) -> bool:
        async with httpx.AsyncClient(timeout=2.0) as c:
            r = await c.get(f"{self.base_url}/readyz")
            return r.status_code == 200

    async def create_collection(self, name: str, *, vector_size: int, distance: str = "Cosine") -> None:
        body = {"vectors": {"size": vector_size, "distance": distance}}
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.put(f"{self.base_url}/collections/{name}", json=body)
            # 409 if exists
            if r.status_code in (200, 201, 202, 409):
                return
            raise RuntimeError(f"create_collection failed: {r.status_code} {r.text}")

    async def upsert_points(self, collection: str, points: List[Dict[str, Any]]) -> None:
        body = {"points": points}
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.put(f"{self.base_url}/collections/{collection}/points", params={"wait": "true"}, json=body)
            if r.status_code in (200, 201, 202):
                return
            raise RuntimeError(f"upsert_points failed: {r.status_code} {r.text}")

    async def search(self, collection: str, vector: List[float], limit: int = 5) -> List[Dict[str, Any]]:
        body = {"vector": vector, "limit": limit, "with_payload": True}
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.post(f"{self.base_url}/collections/{collection}/points/search", json=body)
            r.raise_for_status()
            data = r.json()
            return data.get("result", [])

    async def list_aliases(self) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient(timeout=5.0) as c:
            r = await c.get(f"{self.base_url}/collections/aliases")
            r.raise_for_status()
            return r.json().get("result", {}).get("aliases", [])

    async def get_alias_target(self, alias_name: str) -> Optional[str]:
        aliases = await self.list_aliases()
        for a in aliases:
            if a.get("alias_name") == alias_name:
                return a.get("collection_name")
        return None

    async def set_alias(self, alias_name: str, collection_name: str) -> None:
        # delete alias if exists, then create
        actions = [{"delete_alias": {"alias_name": alias_name}}, {"create_alias": {"alias_name": alias_name, "collection_name": collection_name}}]
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.post(f"{self.base_url}/collections/aliases", json={"actions": actions})
            if r.status_code in (200, 201, 202):
                return
            # If delete_alias fails because alias didn't exist, try create only.
            actions2 = [{"create_alias": {"alias_name": alias_name, "collection_name": collection_name}}]
            r2 = await c.post(f"{self.base_url}/collections/aliases", json={"actions": actions2})
            if r2.status_code in (200, 201, 202):
                return
            raise RuntimeError(f"set_alias failed: {r.status_code} {r.text} / {r2.status_code} {r2.text}")
