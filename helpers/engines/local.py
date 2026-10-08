"""Local durable engine — zero-dependency SQLite journal + in-process runner.

Durability model: each task runs an agent-shaped loop (llm_call → tool_calls
→ repeat) where every step is journaled by ``(task_id, step_key)`` BEFORE its
result is trusted. On restart, resumable tasks are re-attached and the
runner replays: journaled steps short-circuit on their memoized result, so
work already done is never re-executed and the loop resumes at the first
un-journaled step. Semantics are AT-LEAST-ONCE in the window between a
step's side effect and its ``step_done`` commit — side-effecting steps must
be idempotent on ``(tool_name, tool_args)`` (the idempotency key is the
dedupe surface).

This IS the "APScheduler+SQLite is enough" path — for tasks that must
survive an a0 restart and don't need multi-host failover, the local engine
has zero moving parts. Scale-out, cross-process signals, and a managed UI
are what the Restate engine is for.

Pause/cancel are persisted in the task row's status so they survive restart;
in-memory asyncio.Events just make the runner respond promptly within a
process. Error model is fail-fast: a step exception fails the task (no
in-engine retries) — restart resilience comes from the journal, not loops.

Single-process engine: two a0 instances sharing one journal_path will
double-execute in-flight steps — there is no cross-process lease.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from usr.plugins.durable.helpers import LOG_NAME, registry
from usr.plugins.durable.helpers.contract import (
    SIGNAL_ACTIONS,
    TERMINAL_VALUES,
    TaskState,
    TaskStatus,
    ToolIdempotencyKey,
    iter_cap,
    loop_inputs,
    normalize_task_input,
    tool_calls_of,
    tool_message,
)
from usr.plugins.durable.helpers.journal import Journal

log = logging.getLogger(LOG_NAME)


class LocalEngine:
    """Engine interface: start/submit/signal/status/resume_incomplete/stop."""

    def __init__(
        self,
        journal: Journal,
        *,
        step_timeout_s: float,
        max_iterations: int,
        max_concurrent: int = 8,
    ) -> None:
        self.journal = journal
        self.step_timeout_s = step_timeout_s
        self.max_iterations = max_iterations
        self.max_concurrent = max(1, int(max_concurrent))
        self._runners: dict[str, asyncio.Task] = {}
        self._wake: dict[str, asyncio.Event] = {}
        self._stopping = False

    # --- engine interface ----------------------------------------------------

    async def start(self) -> bool:
        """Uniform engine start — local needs no serving step."""
        return True

    async def submit(self, task_input: dict[str, Any]) -> str | None:
        """Create the task row idempotently and spawn the runner only when
        the row is new — re-submitted ids (e.g. every job_loop tick) must not
        churn a throwaway runner against an existing/terminal task."""
        try:
            if self._stopping:
                return None
            task_id, task_input = normalize_task_input(task_input)
            if self.journal.create_task(task_id, task_input):
                self.attach(task_id)
            elif self.journal.get_status(task_id) is None:
                # create_task False + no row = the insert failed (bad input,
                # dead journal) — not an idempotent resubmit. Don't claim a
                # task_id for a task that doesn't exist.
                log.warning("local submit: task %s was never persisted", task_id)
                return None
            return task_id
        except Exception as e:
            log.warning("local engine submit failed: %s", e)
            return None

    def attach(self, task_id: str) -> None:
        """Spawn a runner for an existing task if none is live. Over the
        concurrency cap the task stays resumable and the next tick retries —
        no queue machinery, the journal IS the backlog."""
        try:
            if self._stopping:
                return
            existing = self._runners.get(task_id)
            if existing is not None and not existing.done():
                return
            live = sum(1 for t in self._runners.values() if not t.done())
            if live >= self.max_concurrent:
                log.debug("local engine: runner cap %s — deferring %s",
                          self.max_concurrent, task_id)
                return
            loop = asyncio.get_running_loop()
            self._runners[task_id] = loop.create_task(self._run(task_id))
        except RuntimeError:
            log.warning("local engine attach(%s): no running loop", task_id)
        except Exception as e:
            log.warning("local engine attach(%s) failed: %s", task_id, e)

    def resume_incomplete(self) -> None:
        """Re-attach runners for every resumable (non-terminal, non-paused)
        task — the restart path driven by the job_loop tick."""
        for task_id in self.journal.incomplete_tasks():
            self.attach(task_id)

    async def signal(self, task_id: str, action: str) -> bool:
        """pause / resume / cancel — persisted to the task row first, then
        the in-memory wake event so a running task reacts promptly. False on
        unknown/terminal tasks, unknown actions, or a failed persist."""
        try:
            status = self.journal.get_status(task_id)
            if status is None or status in TERMINAL_VALUES:
                return False  # nothing to signal on a terminal/unknown task
            target = SIGNAL_ACTIONS.get(action)
            if target is None:
                return False
            if not self.journal.update_task(task_id, status=target):
                return False  # don't ack a signal that didn't persist
            if action == "resume":
                # no runner may be live (paused tasks don't auto-attach);
                # spawn one now rather than waiting for the next tick
                self.attach(task_id)
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

    async def meta(self, task_id: str) -> dict[str, Any] | None:
        """Bounded status projection for poll paths — no state_json parse."""
        return self.journal.get_meta(task_id)

    async def stop(self) -> None:
        """Cancel live runners and await their teardown — uninstall() needs
        steps settled into a torn ('running') or terminal state before it
        returns. Journaled state makes every survivor resumable. The gather
        is bounded: a step that swallows CancelledError must not hang the
        shutdown path."""
        self._stopping = True  # blocked first — runner teardown re-scans
        runners = [t for t in self._runners.values() if not t.done()]
        for runner in runners:
            try:
                runner.cancel()
            except Exception:
                pass
        if runners:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*runners, return_exceptions=True),
                    timeout=min(self.step_timeout_s, 30),
                )
            except Exception:
                pass  # deadline or cross-loop teardown — journal row is torn
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
            raw_state = row.get("state")
            if raw_state is None:
                # journal surfaced corrupt state_json — never run a task
                # against fabricated/empty state
                log.warning("task %s state_json corrupt — failing closed", task_id)
                self.journal.update_task(task_id, status=TaskStatus.FAILED)
                return
            try:
                state = TaskState.from_dict(raw_state) if raw_state else TaskState(id=task_id)
            except Exception:
                # present but malformed — same fail-closed contract
                log.warning("task %s state_json unreadable — failing", task_id)
                self.journal.update_task(task_id, status=TaskStatus.FAILED)
                return
            if row_status == TaskStatus.PAUSED.value:
                # reached via resume racing a tick — let the gate park without
                # rewriting the identical state_json back
                pass
            else:
                if state.status == TaskStatus.CREATED:
                    state.transition_to(TaskStatus.PLANNED)
                state.transition_to(TaskStatus.EXECUTING)
                # conditional write — a pause/cancel that landed between the
                # status read and this write must not be overwritten
                if not self.journal.update_task(
                    task_id, status=TaskStatus.EXECUTING, state=state,
                    non_terminal_only=True,
                ):
                    return

            messages, model_cfg = loop_inputs(state, task_input)
            max_iter = iter_cap(task_input, self.max_iterations)
            # Iterations <= this boundary already have their tool messages
            # persisted inside checkpointed prompt_messages — replayed steps
            # consume the memo but must NOT append again. `or -1` is a bug
            # here: boundary 0 is a real boundary but falsy.
            boundary = state.context_snapshot.get("checkpointed_through")
            boundary = int(boundary) if boundary is not None else -1

            for iteration in range(max_iter):
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
                    if iteration > boundary:
                        messages.append(tool_message(name, tr))
                    if tr.get("break_loop"):
                        state.context_snapshot["final_response"] = tr
                        self._complete(task_id, state)
                        return
                # checkpoint: persist state incl. updated tool_results/messages
                # (skipped for fully-replayed iterations — nothing changed)
                if iteration > boundary:
                    state.context_snapshot["prompt_messages"] = messages
                    state.context_snapshot["checkpointed_through"] = iteration
                    self.journal.update_task(task_id, state=state)

            # iteration cap reached — Khan semantics: complete
            state.context_snapshot["final_response"] = {
                "note": f"max_iterations ({max_iter}) reached"
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
            if not self._stopping:
                # a slot freed — drain tasks deferred by the concurrency cap
                # (their rows stay resumable; the next tick would get here
                # anyway, this just doesn't wait for it)
                self.resume_incomplete()

    async def _step(self, task_id: str, step_key: str, name: str, **kwargs):
        """Journaled step: memoized hit returns without executing; miss runs
        the registered callable under the step timeout. A result that can't
        serialize fails the step — an unjournaled result is never trusted."""
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
            json.dumps(result)  # must serialize BEFORE the journal trusts it
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
        nothing can interleave until the wait suspends). On exit the row is
        restored to 'executing' — a persisted 'resumed' must not linger as
        the reported status for the rest of the run."""
        ev = self._wake.setdefault(task_id, asyncio.Event())
        while True:
            if self.journal.get_status(task_id) != TaskStatus.PAUSED.value:
                return
            ev.clear()
            if self.journal.get_status(task_id) != TaskStatus.PAUSED.value:
                return
            await ev.wait()
            st = self.journal.get_status(task_id)
            if st != TaskStatus.PAUSED.value:
                # restore 'executing' after a resume — but never overwrite a
                # terminal status (cancel may have landed while parked)
                if st == TaskStatus.RESUMED.value:
                    self.journal.update_task(
                        task_id, status=TaskStatus.EXECUTING,
                        non_terminal_only=True,
                    )
                return

    def _halted(self, task_id: str) -> bool:
        return self.journal.get_status(task_id) in TERMINAL_VALUES

    def _complete(self, task_id: str, state: TaskState) -> None:
        # a cancel that landed during the final in-flight step wins — never
        # flip FAILED back to COMPLETED
        if self._halted(task_id):
            return
        try:
            state.transition_to(TaskStatus.COMPLETED)
        except Exception:
            pass
        # status commits FIRST — a poisoned state can't roll back the
        # terminal write (update_task serializes outside the txn)
        self.journal.update_task(task_id, status=TaskStatus.COMPLETED)
        self.journal.update_task(task_id, state=state)

    def _fail(self, task_id: str, state: TaskState) -> None:
        if self._halted(task_id):
            return
        try:
            state.transition_to(TaskStatus.FAILED)
        except Exception:
            pass
        self.journal.update_task(task_id, status=TaskStatus.FAILED)
        self.journal.update_task(task_id, state=state)
