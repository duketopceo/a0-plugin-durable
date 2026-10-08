"""Local durable engine — zero-dependency SQLite journal + in-process runner.

Durability model: each task runs an agent-shaped loop (llm_call → tool_calls
→ repeat) where every step is journaled by ``(task_id, step_key)`` BEFORE its
result is trusted. On restart, ``incomplete_tasks`` are re-attached and the
runner replays: journaled steps short-circuit on their memoized result, so
work already done is never re-executed and the loop resumes at the first
un-journaled step.

This IS the "APScheduler+SQLite is enough" path — for tasks that must
survive an a0 restart and don't need multi-host failover, the local engine
has zero moving parts. Scale-out, cross-process signals, and a managed UI
are what the Restate engine is for.

Pause/cancel are persisted in the task row's status so they survive restart;
in-memory asyncio.Events just make the runner respond promptly within a
process.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from usr.plugins.durable.helpers import LOG_NAME, registry
from usr.plugins.durable.helpers.contract import (
    SIGNAL_ACTIONS,
    TERMINAL_VALUES,
    TaskState,
    TaskStatus,
    ToolIdempotencyKey,
    loop_inputs,
    normalize_task_input,
    tool_calls_of,
    tool_message,
)
from usr.plugins.durable.helpers.journal import Journal

log = logging.getLogger(LOG_NAME)


class LocalEngine:
    """Engine interface: submit/signal/status/attach/resume_incomplete/stop."""

    def __init__(self, journal: Journal, *, step_timeout_s: float, max_iterations: int) -> None:
        self.journal = journal
        self.step_timeout_s = step_timeout_s
        self.max_iterations = max_iterations
        self._runners: dict[str, asyncio.Task] = {}
        self._wake: dict[str, asyncio.Event] = {}

    # --- engine interface ----------------------------------------------------

    async def submit(self, task_input: dict[str, Any]) -> str | None:
        """Create the task row idempotently and spawn the runner only when
        the row is new — re-submitted ids (e.g. every job_loop tick) must not
        churn a throwaway runner against an existing/terminal task."""
        try:
            task_id, task_input = normalize_task_input(task_input)
            if self.journal.create_task(task_id, task_input):
                self.attach(task_id)
            return task_id
        except Exception as e:
            log.warning("local engine submit failed: %s", e)
            return None

    def attach(self, task_id: str) -> None:
        """Spawn a runner for an existing task if none is live."""
        try:
            existing = self._runners.get(task_id)
            if existing is not None and not existing.done():
                return
            loop = asyncio.get_running_loop()
            self._runners[task_id] = loop.create_task(self._run(task_id))
        except RuntimeError:
            log.warning("local engine attach(%s): no running loop", task_id)
        except Exception as e:
            log.warning("local engine attach(%s) failed: %s", task_id, e)

    def resume_incomplete(self) -> None:
        """Re-attach runners for every non-terminal task — the restart path
        driven by the job_loop tick."""
        for task_id in self.journal.incomplete_tasks():
            self.attach(task_id)

    async def signal(self, task_id: str, action: str) -> bool:
        """pause / resume / cancel — persisted to the task row first, then
        the in-memory wake event so a running task reacts promptly."""
        try:
            status = self.journal.get_status(task_id)
            if status is None or status in TERMINAL_VALUES:
                return False  # nothing to signal on a terminal/unknown task
            target = SIGNAL_ACTIONS.get(action)
            if target is None:
                return False
            self.journal.update_task(task_id, status=target)
            ev = self._wake.get(task_id)
            if ev is not None:
                ev.set()
            return True
        except Exception as e:
            log.warning("local engine signal(%s,%s) failed: %s", task_id, action, e)
            return False

    async def status(self, task_id: str) -> dict[str, Any] | None:
        row = self.journal.get_task(task_id)
        if row is None:
            return None
        state = row.get("state") or {}
        state.setdefault("id", task_id)
        # the status column is authoritative — state_json may lag a signal
        state["status"] = row["status"]
        return state

    async def stop(self) -> None:
        """Cancel live runners and await their teardown — uninstall() needs
        steps settled into a torn ('running') or terminal state before it
        returns. Journaled state makes every survivor resumable."""
        runners = [t for t in self._runners.values() if not t.done()]
        for runner in runners:
            try:
                runner.cancel()
            except Exception:
                pass
        if runners:
            await asyncio.gather(*runners, return_exceptions=True)
        self._runners.clear()
        self._wake.clear()
        close = getattr(self.journal, "close", None)
        if close is not None:
            close()

    # --- runner ---------------------------------------------------------------

    async def _run(self, task_id: str) -> None:
        """Agent-loop-shaped durable runner. All journal/state errors are
        contained: worst case the task lands FAILED, never the host."""
        try:
            row_status = self.journal.get_status(task_id)
            if row_status is None or row_status in TERMINAL_VALUES:
                return
            row = self.journal.get_task(task_id) or {}
            task_input = row.get("input") or {}
            state = TaskState.from_dict(row["state"]) if row.get("state") else TaskState(id=task_id)
            if row_status == TaskStatus.PAUSED.value:
                # paused at shutdown must STAY paused — hydrate the checkpoint
                # but do not flip the row; _await_if_paused parks on it
                self.journal.update_task(task_id, state=state)
            else:
                if state.status == TaskStatus.CREATED:
                    state.transition_to(TaskStatus.PLANNED)
                state.transition_to(TaskStatus.EXECUTING)
                self.journal.update_task(task_id, status=TaskStatus.EXECUTING, state=state)

            messages, model_cfg = loop_inputs(state, task_input)

            for iteration in range(self.max_iterations):
                if self._halted(task_id):
                    return
                await self._await_if_paused(task_id)
                if self._halted(task_id):
                    return

                llm_result = await self._step(
                    task_id, f"iter{iteration}:llm", "llm_call",
                    prompt_messages=messages, model_config=model_cfg,
                )
                if llm_result is None:  # step failed
                    self._fail(task_id, state)
                    return

                tool_calls = tool_calls_of(llm_result)
                if not tool_calls:
                    state.context_snapshot["final_response"] = llm_result
                    self._complete(task_id, state)
                    return

                for tc in tool_calls:
                    if self._halted(task_id):
                        return
                    await self._await_if_paused(task_id)
                    name = str(tc.get("name", ""))
                    args = dict(tc.get("args", {}) or {})
                    key = ToolIdempotencyKey.build(name, args)
                    # same idempotency key => same memoized result (par with
                    # the restate engine) — on top of the step journal
                    tr = state.tool_results.get(key)
                    if tr is None:
                        tr = await self._step(
                            task_id, f"iter{iteration}:tool:{key}", "tool_call",
                            tool_name=name, tool_args=args, idempotency_key=key,
                        )
                    if tr is None:
                        self._fail(task_id, state)
                        return
                    state.tool_results[key] = tr
                    state.touch()
                    messages.append(tool_message(name, tr))
                    if tr.get("break_loop"):
                        state.context_snapshot["final_response"] = tr
                        self._complete(task_id, state)
                        return
                # checkpoint: persist state incl. updated tool_results/messages
                state.context_snapshot["prompt_messages"] = messages
                self.journal.update_task(task_id, state=state)

            # iteration cap reached — Khan semantics: complete
            state.context_snapshot["final_response"] = {
                "note": f"max_iterations ({self.max_iterations}) reached"
            }
            self._complete(task_id, state)
        except asyncio.CancelledError:
            raise  # stop() cancels runners; not a task failure
        except Exception as e:
            log.warning("local runner for %s crashed: %s", task_id, e)
            self.journal.update_task(task_id, status=TaskStatus.FAILED)
        finally:
            self._runners.pop(task_id, None)
            self._wake.pop(task_id, None)

    async def _step(self, task_id: str, step_key: str, name: str, **kwargs):
        """Journaled step: memoized hit returns without executing; miss runs
        the registered callable under the step timeout."""
        memo = self.journal.step_result(task_id, step_key)
        if memo is not None:
            return memo
        fn = registry.get_step(name)
        if fn is None:
            self.journal.step_failed(task_id, step_key, name, f"no step registered: {name}")
            return None
        self.journal.step_begin(task_id, step_key, name)
        try:
            result = await asyncio.wait_for(fn(**kwargs), timeout=self.step_timeout_s)
            if not isinstance(result, dict):
                raise TypeError(f"step {name} returned non-dict")
            self.journal.step_done(task_id, step_key, name, result)
            return result
        except asyncio.CancelledError:
            raise  # runner shutdown — leave the step 'running'; init resets it
        except Exception as e:
            self.journal.step_failed(task_id, step_key, name, str(e))
            return None

    async def _await_if_paused(self, task_id: str) -> None:
        """Park while the row is paused. The journal is single-process, so
        signal() always sets this Event — a bare wait cannot miss a wake.
        The second status check sits between clear() and the await so a
        resume racing the clear can't be lost (no yield between them —
        nothing can interleave until the wait suspends)."""
        ev = self._wake.setdefault(task_id, asyncio.Event())
        while True:
            if self.journal.get_status(task_id) != TaskStatus.PAUSED.value:
                return
            ev.clear()
            if self.journal.get_status(task_id) != TaskStatus.PAUSED.value:
                return
            await ev.wait()

    def _halted(self, task_id: str) -> bool:
        return self.journal.get_status(task_id) in TERMINAL_VALUES

    def _complete(self, task_id: str, state: TaskState) -> None:
        state.transition_to(TaskStatus.COMPLETED)
        self.journal.update_task(task_id, status=TaskStatus.COMPLETED, state=state)

    def _fail(self, task_id: str, state: TaskState) -> None:
        try:
            state.transition_to(TaskStatus.FAILED)
        except Exception:
            pass
        self.journal.update_task(task_id, status=TaskStatus.FAILED, state=state)
