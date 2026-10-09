"""Query a durable task's state.

POST /api/plugins/durable/durable_status — body: {"task_id": "..."}.
Returns a bounded projection (id/status/timestamps) by default; pass
{"full": true} for the serialized TaskState (local engine) or workflow state
(restate) — full state includes prompt history and tool results, so it is
opt-in.
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
        if not isinstance(input, dict):
            return {"ok": False, "error": "object body required"}
        task_id = str(input.get("task_id") or "").strip()
        if not task_id:
            return {"ok": False, "error": "task_id required"}
        if input.get("full"):
            state = await runtime.status(task_id)
        else:
            state = await runtime.meta(task_id)
        if state is None:
            return {"ok": False, "error": "unknown task"}
        return {"ok": True, "state": state}
