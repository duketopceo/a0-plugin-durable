"""Signal a durable task: pause / resume / cancel.

POST /api/plugins/durable/durable_signal — body: {"task_id": "...", "action":
"pause"|"resume"|"cancel"}.
"""

from __future__ import annotations

from helpers.api import ApiHandler


class DurableSignal(ApiHandler):
    @classmethod
    def get_methods(cls):
        return ["POST"]

    async def process(self, input, request):
        from usr.plugins.durable.helpers import runtime

        if not runtime.is_active():
            return {"ok": False, "error": "durable inactive (disabled or engine unavailable)"}
        task_id = str((input or {}).get("task_id") or "").strip()
        action = str((input or {}).get("action") or "").strip().lower()
        if not task_id or action not in ("pause", "resume", "cancel"):
            return {"ok": False, "error": "task_id + action (pause|resume|cancel) required"}
        ok = await runtime.signal(task_id, action)
        return {"ok": ok} if ok else {"ok": False, "error": "signal failed — see server logs"}
