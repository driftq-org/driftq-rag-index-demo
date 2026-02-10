from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Literal, Optional

FailMode = Literal["never", "once", "always"]

class BuildRequest(BaseModel):
    index: str = Field(default="demo", min_length=1)
    dataset: str = Field(default="sample", min_length=1)
    # Failure injection (for demo)
    fail_step: str = Field(default="none")
    fail_mode: FailMode = Field(default="never")

class ReplayRequest(BaseModel):
    run_id: str = Field(..., min_length=8)
    from_step: str = Field(default="embed")
    # Optional new failure behavior during replay
    fail_step: str = Field(default="none")
    fail_mode: FailMode = Field(default="never")

class RollbackRequest(BaseModel):
    index: str = Field(default="demo", min_length=1)
    # Roll back N versions (1 = previous)
    steps: int = Field(default=1, ge=1)
    # Or explicitly pick a version (takes precedence if provided)
    to_version: Optional[int] = Field(default=None, ge=1)

class EnqueueResponse(BaseModel):
    run_id: str
    queued: bool
    topic: str
