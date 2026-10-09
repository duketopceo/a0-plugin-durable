"""Engine protocol — the single definition of what a durable engine is.

Runtime consumes exactly this surface; a third engine (e.g. Hatchet)
implements it and registers one branch in ``runtime.configure()``.
``attach`` is intentionally NOT part of the contract — it's a local-engine
internal detail.
"""

from __future__ import annotations

from typing import Any, Protocol


class Engine(Protocol):
    async def start(self) -> bool:
        """Bring up any serving side the engine needs. Idempotent; False
        means 'not available' — callers retry on next async touch."""
        ...

    async def submit(self, task_input: dict[str, Any]) -> str | None:
        """Persist + launch a task; returns the task id or None. Must be
        idempotent on ``task_input['id']`` — job_loop re-submits every tick."""
        ...

    async def signal(self, task_id: str, action: str) -> bool:
        """pause|resume|cancel. False on unknown/terminal task or bad verb."""
        ...

    async def status(self, task_id: str) -> dict[str, Any] | None:
        """Serialized TaskState dict, or None when the task is unknown."""
        ...

    async def meta(self, task_id: str) -> dict[str, Any] | None:
        """Bounded status projection for poll paths (id/status/timestamps)."""
        ...

    def resume_incomplete(self) -> None:
        """Re-attach non-terminal work after a restart. Engines that replay
        server-side (Restate) implement this as a no-op."""
        ...

    async def stop(self) -> None:
        """Stop engine-owned services; durable state must survive."""
        ...
