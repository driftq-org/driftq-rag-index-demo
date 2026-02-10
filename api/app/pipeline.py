from __future__ import annotations

import hashlib
import json
import os
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .storage import run_dir, append_log, write_json, read_json

def stable_hash(s: str) -> int:
    return int(hashlib.sha256(s.encode("utf-8")).hexdigest()[:16], 16)

def fake_embed(text: str, dim: int) -> List[float]:
    # Deterministic pseudo-embedding: hash -> floats in [0,1)
    h = hashlib.sha256(text.encode("utf-8")).digest()
    # Expand to dim by repeated hashing
    out: List[float] = []
    seed = h
    while len(out) < dim:
        seed = hashlib.sha256(seed).digest()
        for b in seed:
            out.append(b / 255.0)
            if len(out) >= dim:
                break
    return out[:dim]

def load_docs(dataset: str) -> List[Dict[str, Any]]:
    # expects /data/docs/*
    root = Path("/data/docs")
    docs: List[Dict[str, Any]] = []
    for p in sorted(root.glob("*.md")):
        docs.append({"doc_id": p.stem, "path": str(p), "text": p.read_text(encoding="utf-8")})
    if not docs:
        raise RuntimeError(f"No docs found in {root} (dataset={dataset})")
    return docs

def chunk_text(text: str, *, chunk_size: int = 600, overlap: int = 80) -> List[str]:
    s = text.strip().replace("\r\n", "\n")
    if len(s) <= chunk_size:
        return [s]
    chunks=[]
    i=0
    while i < len(s):
        j=min(len(s), i+chunk_size)
        chunks.append(s[i:j])
        if j==len(s):
            break
        i = max(0, j - overlap)
    return chunks

def discover_step(run_id: str, dataset: str) -> Path:
    docs = load_docs(dataset)
    out = run_dir(run_id) / "docs.json"
    write_json(out, docs)
    append_log(run_id, f"discover: found {len(docs)} docs")
    return out

def chunk_step(run_id: str) -> Path:
    docs_path = run_dir(run_id) / "docs.json"
    docs = read_json(docs_path, default=[])
    chunks=[]
    for d in docs:
        parts = chunk_text(d["text"])
        for idx, part in enumerate(parts):
            chunks.append({
                "chunk_id": f"{d['doc_id']}-{idx}",
                "doc_id": d["doc_id"],
                "text": part,
            })
    out = run_dir(run_id) / "chunks.json"
    write_json(out, chunks)
    append_log(run_id, f"chunk: produced {len(chunks)} chunks")
    return out

def embed_step(run_id: str, dim: int) -> Path:
    chunks_path = run_dir(run_id) / "chunks.json"
    chunks = read_json(chunks_path, default=[])
    embeds=[]
    for c in chunks:
        embeds.append({
            "chunk_id": c["chunk_id"],
            "vector": fake_embed(c["text"], dim),
            "doc_id": c["doc_id"],
            "text": c["text"][:200],
        })
    out = run_dir(run_id) / "embeddings.json"
    write_json(out, embeds)
    append_log(run_id, f"embed: generated {len(embeds)} embeddings (dim={dim})")
    return out

def should_fail(*, fail_step: str, fail_mode: str, step: str, marker_path: Path) -> bool:
    if fail_step != step:
        return False
    if fail_mode == "never":
        return False
    if fail_mode == "always":
        return True
    # once
    if marker_path.exists():
        return False
    marker_path.write_text("failed-once", encoding="utf-8")
    return True
