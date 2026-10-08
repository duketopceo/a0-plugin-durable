"""Submit a durable task.

POST /api/plugins/durable/durable_submit — body: {"input": {...}} or the
task input fields at top level. Returns {"ok", "task_id"}.
"""

from __future__ import annotations

from helpers.api import ApiHandler


class DurableSubmit(ApiHandler):
    @classmethod
    def get_methods(cls):
        return ["POST"]

    async def process(self, input, request):
        from usr.plugins.durable.helpers import runtime

        if not runtime.is_active():
            return {"ok": False, "error": "durable inactive (disabled or engine unavailable)"}
        task_input = input.get("input") if isinstance(input, dict) else None
        if not isinstance(task_input, dict):
            task_input = input if isinstance(input, dict) else {}
        task_id = await runtime.submit(task_input)
        if task_id is None:
            return {"ok": False, "error": "submit failed — see server logs"}
        return {"ok": True, "task_id": task_id}
