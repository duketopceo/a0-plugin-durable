"""Restate engine — real durable execution via a Restate server sidecar.

Model: the plugin serves a ``restate.Workflow`` ("AgentTask") on an
in-process ASGI endpoint thread; the Restate server (single Rust binary,
embedded RocksDB — run it beside a0, see README) invokes handlers back into
this process and journals every ``ctx.run`` step. If a0 dies, Restate
replays the journal and re-invokes — journaled steps skip execution, so the
workflow resumes where it left off. "Process restart" survival is real
durability, not emulation.

Step callables come from the host registry (`helpers/registry.py`), resolved
by NAME inside the handler — never serialized (callables can't cross the
wire). Submission/status/signals go over the stdlib urllib ingress client —
the SDK is only needed on the serving side.

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

from usr.plugins.durable.helpers import LOG_NAME, registry
from usr.plugins.durable.helpers.contract import (
    SIGNAL_ACTIONS,
    TaskState,
    TaskStatus,
    ToolIdempotencyKey,
    json_or,
    loop_inputs,
    normalize_task_input,
    state_or_new,
    tool_calls_of,
    tool_message,
)

log = logging.getLogger(LOG_NAME)

try:
    import restate
    from restate import WorkflowContext, WorkflowSharedContext
except ModuleNotFoundError:
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
except ModuleNotFoundError:
    _hc_serve = None  # type: ignore[assignment]
    _HcConfig = None  # type: ignore[assignment]


# --- the durable workflow ----------------------------------------------------
# Module scope so restate.app() can bind it; the step registry is looked up
# lazily inside handlers so the module stays importable without the SDK.

if restate is not None:

    agent_task = restate.Workflow("AgentTask")

    async def _wf_fail(ctx: WorkflowContext, state: TaskState) -> dict:
        state.transition_to(TaskStatus.FAILED)
        await ctx.set("state", state.to_dict())
        return state.to_dict()

    async def _wf_complete(ctx: WorkflowContext, state: TaskState) -> dict:
        state.transition_to(TaskStatus.COMPLETED)
        await ctx.set("state", state.to_dict())
        return state.to_dict()

    async def _wf_pause_gate(ctx: WorkflowContext) -> bool:
        """Park while paused. Durable promise per pause epoch — zero journal
        churn vs a ctx.sleep poll (a day-paused task would append ~86k
        entries). Returns True when cancelled so callers exit promptly."""
        while await ctx.get("paused") and not await ctx.get("cancelled"):
            epoch = await ctx.get("pause_epoch") or 0
            await ctx.promise(f"resume_{epoch}").value()
        return bool(await ctx.get("cancelled"))

    @agent_task.main()
    async def _run_workflow(ctx: WorkflowContext, task_input: dict) -> dict:
        """Agent-loop workflow: llm_call → tool_calls → repeat. Every step is
        a journaled ctx.run — replayed invocations skip completed steps."""
        state = state_or_new(task_input)
        max_iter = int(task_input.get("max_iterations") or 100)
        if state.status == TaskStatus.CREATED:
            state.transition_to(TaskStatus.PLANNED)
        state.transition_to(TaskStatus.EXECUTING)
        await ctx.set("state", state.to_dict())

        messages, model_cfg = loop_inputs(state, task_input)

        for iteration in range(max_iter):
            if await ctx.get("cancelled"):
                return await _wf_fail(ctx, state)
            if await _wf_pause_gate(ctx):
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
                if await ctx.get("cancelled"):
                    return await _wf_fail(ctx, state)
                if await _wf_pause_gate(ctx):
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
                messages.append(tool_message(name, tr))
                if tr.get("break_loop"):
                    state.context_snapshot["final_response"] = tr
                    return await _wf_complete(ctx, state)

            state.context_snapshot["prompt_messages"] = messages
            await ctx.set("state", state.to_dict())

        state.context_snapshot["final_response"] = {
            "note": f"max_iterations ({max_iter}) reached"
        }
        return await _wf_complete(ctx, state)

    # Shared handlers declare a required dict input — the ingress always
    # carries a JSON body (we POST "{}"), so the signature must accept it.
    @agent_task.handler()
    async def pause(ctx: WorkflowSharedContext, _req: dict) -> None:
        await ctx.set("pause_epoch", (await ctx.get("pause_epoch") or 0) + 1)
        await ctx.set("paused", True)

    @agent_task.handler()
    async def resume(ctx: WorkflowSharedContext, _req: dict) -> None:
        await ctx.set("paused", False)
        epoch = await ctx.get("pause_epoch") or 0
        await ctx.resolve_promise(f"resume_{epoch}", True)

    @agent_task.handler()
    async def cancel(ctx: WorkflowSharedContext, _req: dict) -> None:
        await ctx.set("cancelled", True)
        await ctx.set("paused", False)
        epoch = await ctx.get("pause_epoch") or 0
        await ctx.resolve_promise(f"resume_{epoch}", True)

    @agent_task.handler()
    async def get_state(ctx: WorkflowSharedContext, _req: dict) -> dict:
        return await ctx.get("state") or {}

else:  # SDK absent — module still imports; engine start() returns False
    agent_task = None  # type: ignore[assignment]


async def _exec_step(*, name: str, kwargs: dict) -> dict:
    """Resolve a registered host step and run it. A missing step is TERMINAL
    (no retry will conjure the registration); a step's own exception stays
    retryable per Restate semantics."""
    fn = registry.get_step(name)
    if fn is None:
        raise TerminalError(f"no step registered: {name}")
    result = await fn(**kwargs)
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
            body = resp.read()
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
        self._shutdown: threading.Event | None = None
        self._submitted: set[str] = set()
        self._started = False

    @property
    def available(self) -> bool:
        return restate is not None and _hc_serve is not None

    async def start(self) -> bool:
        """Serve the workflow endpoint + register the deployment. Returns
        False (never raises) when deps are missing or serving fails."""
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
            shutdown = threading.Event()
            self._shutdown = shutdown

            def _serve():
                async def _main():
                    # bridge threading.Event -> asyncio for hypercorn's trigger
                    loop_trigger = asyncio.Event()
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
            registered = await asyncio.to_thread(
                _post,
                f"{self.admin}/deployments",
                {"uri": f"http://127.0.0.1:{self.listen_port}", "force": True},
            )
            self._started = True
            if registered is None:
                log.warning(
                    "restate endpoint up on :%s but admin registration failed — "
                    "register manually: POST %s/deployments", self.listen_port, self.admin
                )
            return True
        except Exception as e:
            log.warning("restate engine start failed: %s", e)
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
                _post, f"{self.ingress}/AgentTask/{task_id}/run/send", task_input
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
        if action not in SIGNAL_ACTIONS:
            return False
        resp = await asyncio.to_thread(
            _post, f"{self.ingress}/AgentTask/{task_id}/{action}", {}
        )
        return resp is not None

    async def status(self, task_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(
            _post, f"{self.ingress}/AgentTask/{task_id}/get_state", {}
        )

    async def stop(self) -> None:
        if self._shutdown is not None:
            self._shutdown.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._started = False
