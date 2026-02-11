from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

STATE_ROOT = Path(os.getenv("DEMO_STATE_DIR", "/state"))

def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def run_dir(run_id: str) -> Path:
    p = STATE_ROOT / "runs" / run_id
    _ensure_dir(p)
    return p

def index_dir(index: str) -> Path:
    p = STATE_ROOT / "indexes" / index
    _ensure_dir(p)
    return p

def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))

def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")

def append_log(run_id: str, line: str) -> None:
    p = run_dir(run_id) / "log.txt"
    ts = now_iso()
    with p.open("a", encoding="utf-8") as f:
        f.write(f"[{ts}] {line}\n")

def get_run_state(run_id: str) -> Dict[str, Any]:
    p = run_dir(run_id) / "state.json"
    return read_json(p, default={"run_id": run_id, "status": "UNKNOWN", "steps": {}, "updated_at": now_iso()})

def set_run_state(run_id: str, state: Dict[str, Any]) -> None:
    state["updated_at"] = now_iso()
    p = run_dir(run_id) / "state.json"
    write_json(p, state)

def init_run_state(*, run_id: str, index: str, dataset: str, version: int, fail_step: str, fail_mode: str) -> Dict[str, Any]:
    state = {
        "run_id": run_id,
        "status": "QUEUED",
        "index": index,
        "dataset": dataset,
        "version": version,
        "fail_step": fail_step,
        "fail_mode": fail_mode,
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "steps": {
            "discover": {"status": "PENDING"},
            "chunk": {"status": "PENDING"},
            "embed": {"status": "PENDING"},
            "upsert": {"status": "PENDING"},
            "promote": {"status": "PENDING"},
            "smoketest": {"status": "PENDING"},
        },
        "artifacts": {},
        "errors": [],
    }
    set_run_state(run_id, state)
    append_log(run_id, f"Initialized run (index={index}, version={version}, dataset={dataset})")
    return state

def history_path(index: str) -> Path:
    return index_dir(index) / "history.json"

def get_history(index: str) -> Dict[str, Any]:
    return read_json(history_path(index), default={"index": index, "versions": [], "active": None})

def set_history(index: str, hist: Dict[str, Any]) -> None:
    write_json(history_path(index), hist)

def next_version(index: str) -> int:
    hist = get_history(index)
    versions = hist.get("versions", [])
    if not versions:
        return 1
    return int(max(versions)) + 1

def record_version(index: str, version: int) -> None:
    hist = get_history(index)
    versions = set(hist.get("versions", []))
    versions.add(int(version))
    hist["versions"] = sorted(versions)
    set_history(index, hist)

def set_active(index: str, version: int) -> None:
    hist = get_history(index)
    record_version(index, version)
    hist["active"] = int(version)
    set_history(index, hist)

def previous_version(index: str, *, steps: int = 1) -> Optional[int]:
    hist = get_history(index)
    active = hist.get("active")
    if active is None:
        return None
    versions = list(hist.get("versions", []))
    if active not in versions:
        return None
    i = versions.index(active)
    j = i - steps
    if j < 0:
        return None
    return versions[j]
