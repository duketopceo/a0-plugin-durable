"""Query a durable task's state.

POST /api/plugins/durable/durable_status — body: {"task_id": "..."}.
Returns the serialized TaskState (local engine) or workflow state (restate).
"""

from __future__ import annotations

from helpers.api import ApiHandler


class DurableStatus(ApiHandler):
    @classmethod
    def get_methods(cls):
        return ["POST"]

    async def process(self, input, request):
        from usr.plugins.durable.helpers import runtime

        if not runtime.is_active():
            return {"ok": False, "error": "durable inactive (disabled or engine unavailable)"}
        task_id = str((input or {}).get("task_id") or "").strip()
        if not task_id:
            return {"ok": False, "error": "task_id required"}
        state = await runtime.status(task_id)
        if state is None:
            return {"ok": False, "error": "unknown task"}
        return {"ok": True, "state": state}
