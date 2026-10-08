"""Restate engine — real durable execution via a Restate server sidecar.

Model: the plugin serves a ``restate.Workflow`` ("AgentTask") on an
in-process ASGI endpoint thread; the Restate server (single Rust binary,
embedded RocksDB — run it beside a0 on loopback, see README) invokes
handlers back into this process and journals every ``ctx.run`` step. If a0
dies, Restate replays the journal and re-invokes — journaled steps skip
execution, so the workflow resumes where it left off. "Process restart"
survival is real durability, not emulation.

Step callables come from the host registry (`helpers/registry.py`), resolved
by NAME inside the handler — never serialized (callables can't cross the
wire). Submission/status/signals go over the stdlib urllib ingress client —
the SDK is only needed on the serving side.

Signals ride durable promises: ``WorkflowSharedContext`` is READ-ONLY for
K/V state (``ctx.set`` is only legal in the run handler), so pause/resume/
cancel resolve promises the workflow gate observes. Promise names are
versioned by a ``pause_cycle`` counter owned by the run handler — durable
promises resolve once, so a fixed name could only carry a single pause.

Error model matches the local engine — fail-fast: a step exception is
converted to TerminalError inside ``ctx.run`` (the task fails, it does NOT
retry forever) and workflow-body errors land in ``_wf_fail`` so the shared
``get_state`` view never reports a phantom 'executing'.

Dependencies are OPTIONAL: this module imports cleanly without
``restate_sdk``/``hypercorn``; :class:`RestateEngine` degrades to inactive
at start() so runtime.configure fails safe.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote

from usr.plugins.durable.helpers import LOG_NAME, registry
from usr.plugins.durable.helpers.contract import (
    SIGNAL_ACTIONS,
    TERMINAL_VALUES,
    TaskState,
    TaskStatus,
    ToolIdempotencyKey,
    iter_cap,
    json_or,
    loop_inputs,
    normalize_task_input,
    tool_calls_of,
    tool_message,
)

log = logging.getLogger(LOG_NAME)

try:
    import restate
    from restate import WorkflowContext, WorkflowSharedContext
except ModuleNotFoundError as exc:
    if exc.name != "restate":
        raise  # a broken install's missing subdep must surface, not masquerade
    restate = None  # type: ignore[assignment]

# TerminalError location varies across restate-sdk versions — resolve it
# without letting a missing symbol cascade into "SDK unavailable".
if restate is not None:
    try:
        from restate.exceptions import TerminalError
    except Exception:
        TerminalError = getattr(restate, "TerminalError", RuntimeError)
else:
    TerminalError = RuntimeError  # type: ignore[assignment,misc]

try:
    from hypercorn.asyncio import serve as _hc_serve
    from hypercorn.config import Config as _HcConfig
except ModuleNotFoundError as exc:
    if exc.name != "hypercorn":
        raise
    _hc_serve = None  # type: ignore[assignment]
    _HcConfig = None  # type: ignore[assignment]


# --- the durable workflow ----------------------------------------------------
# Module scope so restate.app() can bind it; the step registry is looked up
# lazily inside handlers so the module stays importable without the SDK.

if restate is not None:

    agent_task = restate.Workflow("AgentTask")

    async def _peek(ctx: WorkflowContext, name: str):
        """Peek a durable promise's resolved value (or None). Raises
        TerminalError when the SDK lacks peek — a failed task beats a
        silently parked one."""
        promise = ctx.promise(name)
        peek = getattr(promise, "peek", None)
        if peek is None:
            raise TerminalError("restate_sdk lacks DurablePromise.peek — upgrade")
        return await peek()

    async def _peek_shared(ctx: WorkflowSharedContext, name: str):
        promise = ctx.promise(name)
        peek = getattr(promise, "peek", None)
        if peek is None:
            raise TerminalError("restate_sdk lacks DurablePromise.peek — upgrade")
        return await peek()

    async def _wf_fail(ctx: WorkflowContext, state: TaskState) -> dict:
        try:
            state.transition_to(TaskStatus.FAILED)
        except Exception:
            pass
        await ctx.set("state", state.to_dict())
        return state.to_dict()

    async def _wf_complete(ctx: WorkflowContext, state: TaskState) -> dict:
        if await _peek(ctx, "cancel") is not None:
            return await _wf_fail(ctx, state)  # a landed cancel wins
        try:
            state.transition_to(TaskStatus.COMPLETED)
        except Exception:
            pass
        await ctx.set("state", state.to_dict())
        return state.to_dict()

    async def _wf_pause_gate(ctx: WorkflowContext, state: TaskState) -> bool:
        """Park while a pause is pending; returns True when cancelled.

        ``pause_{cycle}`` resolved => a pause was signaled at this cycle;
        the gate then parks on ``resume_{cycle}`` — which ``cancel`` also
        resolves to unstick a parked workflow. Cycle is owned here (the run
        handler is the only ctx that can set), so a double-pause resolves
        the SAME promise name the gate is parked on — no wedge."""
        if await _peek(ctx, "cancel") is not None:
            return True
        cycle = int(await ctx.get("pause_cycle") or 0)
        if await _peek(ctx, f"pause_{cycle}") is None:
            return False
        state.status = TaskStatus.PAUSED
        await ctx.set("state", state.to_dict())
        await ctx.promise(f"resume_{cycle}").value()
        await ctx.set("pause_cycle", cycle + 1)
        state.status = TaskStatus.EXECUTING
        await ctx.set("state", state.to_dict())
        return await _peek(ctx, "cancel") is not None

    @agent_task.main()
    async def _run_workflow(ctx: WorkflowContext, task_input: dict) -> dict:
        """Agent-loop workflow: llm_call → tool_calls → repeat. Every step is
        a journaled ctx.run — replayed invocations skip completed steps.
        Body errors are contained: the task lands FAILED, never 'executing'
        forever."""
        state = TaskState(id=str(task_input.get("id", "")))
        try:
            max_iter = iter_cap(task_input, 100)
            if state.status == TaskStatus.CREATED:
                state.transition_to(TaskStatus.PLANNED)
            state.transition_to(TaskStatus.EXECUTING)
            await ctx.set("state", state.to_dict())

            messages, model_cfg = loop_inputs(state, task_input)
            # `or -1` would discard a real boundary of 0 (falsy)
            boundary = state.context_snapshot.get("checkpointed_through")
            boundary = int(boundary) if boundary is not None else -1

            for iteration in range(max_iter):
                if await _wf_pause_gate(ctx, state):
                    return await _wf_fail(ctx, state)

                llm_result = await ctx.run_typed(
                    "llm_call",
                    _exec_step,
                    name="llm_call",
                    kwargs={"prompt_messages": messages, "model_config": model_cfg},
                )
                tool_calls = tool_calls_of(llm_result)
                if not tool_calls:
                    state.context_snapshot["final_response"] = llm_result
                    return await _wf_complete(ctx, state)

                for tc in tool_calls:
                    if await _wf_pause_gate(ctx, state):
                        return await _wf_fail(ctx, state)
                    name = str(tc.get("name", ""))
                    args = dict(tc.get("args", {}) or {})
                    key = ToolIdempotencyKey.build(name, args)
                    # state-level memo (in addition to the journal): identical
                    # tool calls inside one task never re-execute
                    tr = state.tool_results.get(key)
                    if tr is None:
                        tr = await ctx.run_typed(
                            f"tool:{key}",
                            _exec_step,
                            name="tool_call",
                            kwargs={"tool_name": name, "tool_args": args,
                                    "idempotency_key": key},
                        )
                        state.tool_results[key] = tr
                    if iteration > boundary:
                        messages.append(tool_message(name, tr))
                    if tr.get("break_loop"):
                        state.context_snapshot["final_response"] = tr
                        return await _wf_complete(ctx, state)

                if iteration > boundary:
                    state.context_snapshot["prompt_messages"] = messages
                    state.context_snapshot["checkpointed_through"] = iteration
                    await ctx.set("state", state.to_dict())

            state.context_snapshot["final_response"] = {
                "note": f"max_iterations ({max_iter}) reached"
            }
            return await _wf_complete(ctx, state)
        except Exception as e:
            log.warning("restate workflow %s failed: %s",
                        task_input.get("id"), e)
            return await _wf_fail(ctx, state)

    # Shared handlers are READ-ONLY for K/V state — they signal exclusively
    # through durable promises (the only cross-handler channel). Peeking
    # before resolving keeps repeated signals idempotent: a second pause
    # resolves an already-resolved name, which Restate would reject.
    @agent_task.handler()
    async def pause(ctx: WorkflowSharedContext, _req: dict) -> dict:
        cycle = int(await ctx.get("pause_cycle") or 0)
        if await _peek_shared(ctx, f"pause_{cycle}") is None:
            await ctx.promise(f"pause_{cycle}").resolve(True)
        return {"ok": True}

    @agent_task.handler()
    async def resume(ctx: WorkflowSharedContext, _req: dict) -> dict:
        cycle = int(await ctx.get("pause_cycle") or 0)
        # only resolve when a pause is actually pending — a stray resume must
        # not pre-resolve the name a future park would await
        if await _peek_shared(ctx, f"pause_{cycle}") is not None:
            await ctx.promise(f"resume_{cycle}").resolve(True)
        return {"ok": True}

    @agent_task.handler()
    async def cancel(ctx: WorkflowSharedContext, _req: dict) -> dict:
        if await _peek_shared(ctx, "cancel") is None:
            await ctx.promise("cancel").resolve(True)
        # unstick a parked gate — resume_{cycle} is what it awaits
        cycle = int(await ctx.get("pause_cycle") or 0)
        if await _peek_shared(ctx, f"pause_{cycle}") is not None:
            await ctx.promise(f"resume_{cycle}").resolve(True)
        return {"ok": True}

    @agent_task.handler()
    async def get_state(ctx: WorkflowSharedContext, _req: dict) -> dict | None:
        # None (not {}) — the ingress maps it to "unknown task" parity with
        # the local engine's missing-row None
        return await ctx.get("state")

else:  # SDK absent — module still imports; engine start() returns False
    agent_task = None  # type: ignore[assignment]


async def _exec_step(*, name: str, kwargs: dict) -> dict:
    """Resolve a registered host step and run it. A missing step or a step
    exception is TERMINAL — the local engine fails tasks on first error and
    the same submission must behave identically on restate (deterministic
    bugs must not burn unbounded server-side retries)."""
    fn = registry.get_step(name)
    if fn is None:
        raise TerminalError(f"no step registered: {name}")
    try:
        result = await fn(**kwargs)
    except Exception as e:
        raise TerminalError(f"step {name} failed: {e}") from e
    if not isinstance(result, dict):
        raise TerminalError(f"step {name} returned non-dict")
    return result


# --- ingress client (stdlib — no SDK needed to submit/query) -----------------


def _post(url: str, payload: dict | None, timeout: float = 10.0) -> dict | None:
    """POST JSON to a Restate endpoint; return parsed body or None.
    A parse failure is treated as unreachable — an empty/garbage body must
    not masquerade as success."""
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload or {}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read(65536)
            if not body:
                return {}
            parsed = json_or(body, None)
            return parsed if isinstance(parsed, dict) else None
    except urllib.error.HTTPError as e:
        log.warning("restate ingress %s -> HTTP %s", url, e.code)
        return None
    except Exception as e:
        log.warning("restate ingress %s failed: %s", url, e)
        return None


def _wait_port(port: int, attempts: int = 50, delay: float = 0.1) -> bool:
    """Poll the ASGI endpoint until it accepts connections — the deployment
    registration POST must not race hypercorn's bind."""
    import socket

    for _ in range(attempts):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            import time

            time.sleep(delay)
    return False


class RestateEngine:
    """Engine interface over a Restate server: in-process ASGI endpoint +
    ingress submits. start() requires the optional deps; everything else
    degrades to None."""

    def __init__(
        self,
        *,
        ingress: str,
        admin: str,
        listen_port: int,
        defaults: dict[str, Any] | None = None,
    ) -> None:
        self.ingress = ingress.rstrip("/")
        self.admin = admin.rstrip("/")
        self.listen_port = listen_port
        # config-level fallbacks merged into every submission
        self._defaults = {
            k: v for k, v in (defaults or {}).items() if v not in (None, "")
        }
        self._thread: threading.Thread | None = None
        self._shutdown = threading.Event()  # exists pre-start so stop() is safe
        self._submitted: set[str] = set()   # per-process resubmit dedupe
        self._started = False

    @property
    def available(self) -> bool:
        return restate is not None and _hc_serve is not None

    def _teardown_thread(self) -> None:
        self._shutdown.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        self._shutdown = threading.Event()

    async def start(self) -> bool:
        """Serve the workflow endpoint + register the deployment. Returns
        False (never raises) when deps are missing, the port won't bind, or
        registration fails — a False here lets the next async touch retry
        the whole start instead of leaving a silently unroutable engine."""
        if not self.available:
            log.warning(
                "restate engine requested but restate_sdk/hypercorn are not "
                "installed — `pip install restate_sdk hypercorn`, then restart"
            )
            return False
        if self._started:
            return True
        try:
            app = restate.app([agent_task])
            cfg = _HcConfig()
            cfg.bind = [f"127.0.0.1:{self.listen_port}"]
            cfg.accesslog = None

            def _serve():
                async def _main():
                    # bridge threading.Event -> asyncio for hypercorn's trigger
                    loop_trigger = asyncio.Event()
                    shutdown = self._shutdown
                    async def _watch():
                        await asyncio.to_thread(shutdown.wait)
                        loop_trigger.set()
                    asyncio.create_task(_watch())
                    await _hc_serve(app, cfg, shutdown_trigger=loop_trigger.wait)

                asyncio.run(_main())

            self._thread = threading.Thread(
                target=_serve, name="a0-durable-restate", daemon=True
            )
            self._thread.start()
            if not await asyncio.to_thread(_wait_port, self.listen_port):
                log.warning("restate endpoint never bound :%s", self.listen_port)
                await asyncio.to_thread(self._teardown_thread)
                return False
            registered = await asyncio.to_thread(
                _post,
                f"{self.admin}/deployments",
                {"uri": f"http://127.0.0.1:{self.listen_port}", "force": True},
            )
            if registered is None:
                log.warning(
                    "restate endpoint up on :%s but admin registration failed — "
                    "register manually: POST %s/deployments (will retry on "
                    "next async call)", self.listen_port, self.admin
                )
                await asyncio.to_thread(self._teardown_thread)
                return False
            self._started = True
            return True
        except Exception as e:
            log.warning("restate engine start failed: %s", e)
            await asyncio.to_thread(self._teardown_thread)
            return False

    async def submit(self, task_input: dict[str, Any]) -> str | None:
        """Fire-and-return submit via the keyed send endpoint — the workflow
        key makes re-submission idempotent, and a process-local set skips
        re-POSTing config tasks every tick."""
        try:
            task_id, task_input = normalize_task_input(task_input)
            task_input = {**self._defaults, **task_input}
            if task_id in self._submitted:
                return task_id
            resp = await asyncio.to_thread(
                _post,
                f"{self.ingress}/AgentTask/{quote(task_id, safe='')}/run/send",
                task_input,
            )
            if resp is not None:
                self._submitted.add(task_id)
                return task_id
            return None
        except Exception as e:
            log.warning("restate submit failed: %s", e)
            return None

    def resume_incomplete(self) -> None:
        """No-op — the Restate server replays/resumes independently of a0."""

    async def signal(self, task_id: str, action: str) -> bool:
        """pause/resume/cancel via shared-handler POSTs. Existence + terminal
        gate up front so semantics match the local engine (False on unknown
        or finished tasks)."""
        if action not in SIGNAL_ACTIONS:
            return False
        st = await self.status(task_id)
        if st is None or str(st.get("status")) in TERMINAL_VALUES:
            return False
        resp = await asyncio.to_thread(
            _post,
            f"{self.ingress}/AgentTask/{quote(task_id, safe='')}/{action}",
            {},
        )
        return resp is not None

    async def status(self, task_id: str) -> dict[str, Any] | None:
        resp = await asyncio.to_thread(
            _post,
            f"{self.ingress}/AgentTask/{quote(task_id, safe='')}/get_state",
            {},
        )
        # {} = workflow exists but never set state -> treat as unknown
        return resp if resp else None

    async def meta(self, task_id: str) -> dict[str, Any] | None:
        """Bounded projection — the ctx state IS the payload on restate, so
        meta just trims it to the fields a poll actually needs."""
        state = await self.status(task_id)
        if state is None:
            return None
        return {
            "id": task_id,
            "status": state.get("status"),
            "updated_at": state.get("updated_at"),
        }

    async def stop(self) -> None:
        self._submitted.clear()
        self._shutdown.set()
        if self._thread is not None:
            thread = self._thread
            self._thread = None
            await asyncio.to_thread(thread.join, 5)
        self._shutdown = threading.Event()
        self._started = False
