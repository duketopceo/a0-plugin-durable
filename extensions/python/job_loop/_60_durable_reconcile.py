"""job_loop extension — durable-task reconcile tick.

Submits enabled config tasks (idempotent by task id — re-submission is a
no-op) and, on the local engine, re-attaches runners for non-terminal tasks
so journaled work resumes after an a0 restart. Runs at _60_, after the
housekeeping jobs (_20_/_50_).

Guarded everywhere — a durable failure must never stall the host loop.
Plugin import stays inside execute(): upstream's import_module sweep is
unguarded and a failing module would kill every plugin's job_loop tick.
"""

from helpers.extension import Extension


class DurableReconcile(Extension):
    async def execute(self, **kwargs) -> None:
        del kwargs  # a0 passes no payload at this extension point
        try:
            from usr.plugins.durable.helpers import runtime

            await runtime.tick()
        except Exception:
            pass
