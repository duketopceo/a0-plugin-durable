"""job_loop extension — durable-task reconcile tick.

Submits enabled config tasks (idempotent by task id — re-submission is a
no-op) and, on the local engine, re-attaches runners for non-terminal tasks
so journaled work resumes after an a0 restart. Runs at _60_, after the
housekeeping jobs (_20_/_50_).

Guarded everywhere — a durable failure must never stall the host loop.
"""

from typing import Any

from helpers.extension import Extension

from usr.plugins.durable.helpers import runtime


class DurableReconcile(Extension):
    async def execute(self, data: dict[str, Any] | None = None, **kwargs) -> None:
        try:
            await runtime.tick()
        except Exception:
            pass
