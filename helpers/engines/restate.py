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
``restate_sdk``/``hypercorn``; :class:`RestateEngine` raises RuntimeError at
start() so runtime.configure degrades the plugin to inactive.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import urllib.error
import urllib.request
from datetime import timedelta
from typing import Any

from usr.plugins.durable.helpers import LOG_NAME
from usr.plugins.durable.helpers.contract import TaskState, TaskStatus, ToolIdempotencyKey
from usr.plugins.durable.helpers import registry

log = logging.getLogger(LOG_NAME)

try:
    import restate
    from restate import WorkflowContext, WorkflowSharedContext
except ModuleNotFoundError:
    restate = None  # type: ignore[assignment]

try:
    from hypercorn.asyncio import serve as _hc_serve
    from hypercorn.config import Config as _HcConfig
except ModuleNotFoundError:
    _hc_serve = None  # type: ignore[assignment]
    _HcConfig = None  # type: ignore[assignment]

MAX_ITERATIONS = 100  # mirrors local engine cap; config-driven via input


# --- the durable workflow ----------------------------------------------------
# Module scope so restate.app() can bind it; the step registry is looked up
# lazily inside handlers so the module stays importable without the SDK.

if restate is not None:

    agent_task = restate.Workflow("AgentTask")

    @agent_task.main()
    async def _run_workflow(ctx: WorkflowContext, task_input: dict) -> dict:
        """Agent-loop workflow: llm_call → tool_calls → repeat. Every step is
        a journaled ctx.run — replayed invocations skip completed steps."""
        state = TaskState.from_dict(task_input["state"]) if task_input.get("state") \
            else TaskState(id=str(task_input.get("id", "")))
        max_iter = int(task_input.get("max_iterations", MAX_ITERATIONS))
        step_timeout = float(task_input.get("step_timeout_s", 300))
        if state.status == TaskStatus.CREATED:
            state.transition_to(TaskStatus.PLANNED)
        state.transition_to(TaskStatus.EXECUTING)
        await ctx.set("state", state.to_dict())

        messages = list(state.context_snapshot.get("prompt_messages")
                        or task_input.get("prompt_messages") or [])
        model_cfg = dict(state.model_state.get("model_config")
                         or task_input.get("model_config") or {})

        for iteration in range(max_iter):
            if await ctx.get("cancelled"):
                state.transition_to(TaskStatus.FAILED)
                await ctx.set("state", state.to_dict())
                return state.to_dict()
            while await ctx.get("paused") and not await ctx.get("cancelled"):
                await ctx.sleep(timedelta(seconds=1))  # journaled durable sleep

            llm_result = await ctx.run_typed(
                "llm_call",
                _exec_step,
                name="llm_call",
                kwargs={"prompt_messages": messages, "model_config": model_cfg},
            )
            tool_calls = llm_result.get("tool_calls", []) if isinstance(llm_result, dict) else []
            if not tool_calls:
                state.context_snapshot["final_response"] = llm_result
                state.transition_to(TaskStatus.COMPLETED)
                await ctx.set("state", state.to_dict())
                return state.to_dict()

            for tc in tool_calls:
                if await ctx.get("cancelled"):
                    state.transition_to(TaskStatus.FAILED)
                    await ctx.set("state", state.to_dict())
                    return state.to_dict()
                while await ctx.get("paused") and not await ctx.get("cancelled"):
                    await ctx.sleep(timedelta(seconds=1))
                name = str(tc.get("name", ""))
                args = dict(tc.get("args", {}) or {})
                key = ToolIdempotencyKey.build(name, args)
                # state-level memo (in addition to the journal): identical
                # tool calls inside one task never re-execute
                if key in state.tool_results:
                    tr = state.tool_results[key]
                else:
                    tr = await ctx.run_typed(
                        f"tool:{key}",
                        _exec_step,
                        name="tool_call",
                        kwargs={"tool_name": name, "tool_args": args,
                                "idempotency_key": key},
                    )
                    state.tool_results[key] = tr
                messages.append({"role": "tool", "name": name,
                                 "content": tr.get("result", "")})
                if tr.get("break_loop"):
                    state.context_snapshot["final_response"] = tr
                    state.transition_to(TaskStatus.COMPLETED)
                    await ctx.set("state", state.to_dict())
                    return state.to_dict()

            state.context_snapshot["prompt_messages"] = messages
            state.transition_to(TaskStatus.CHECKPOINTED)
            state.transition_to(TaskStatus.EXECUTING)
            await ctx.set("state", state.to_dict())

        state.context_snapshot["final_response"] = {
            "note": f"max_iterations ({max_iter}) reached"
        }
        state.transition_to(TaskStatus.COMPLETED)
        await ctx.set("state", state.to_dict())
        return state.to_dict()

    @agent_task.handler()
    async def pause(ctx: WorkflowSharedContext, _req: dict | None = None) -> None:
        await ctx.set("paused", True)

    @agent_task.handler()
    async def resume(ctx: WorkflowSharedContext, _req: dict | None = None) -> None:
        await ctx.set("paused", False)

    @agent_task.handler()
    async def cancel(ctx: WorkflowSharedContext, _req: dict | None = None) -> None:
        await ctx.set("cancelled", True)
        await ctx.set("paused", False)

    @agent_task.handler()
    async def get_state(ctx: WorkflowSharedContext, _req: dict | None = None) -> dict:
        return await ctx.get("state") or {}

else:  # SDK absent — module still imports; engine start() raises
    agent_task = None  # type: ignore[assignment]


async def _exec_step(*, name: str, kwargs: dict) -> dict:
    """Resolve a registered host step and run it. Raises KeyError when the
    host never wired the step — Restate treats it as a retryable handler
    error unless the caller marks it terminal."""
    fn = registry.get_step(name)
    if fn is None:
        raise RuntimeError(f"no step registered: {name}")
    result = await fn(**kwargs)
    if not isinstance(result, dict):
        raise TypeError(f"step {name} returned non-dict")
    return result


# --- ingress client (stdlib — no SDK needed to submit/query) -----------------


def _post(url: str, payload: dict | None, timeout: float = 10.0) -> dict | None:
    """POST JSON to a Restate endpoint; return parsed body or None."""
    try:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload or {}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read(65536)
            try:
                return json.loads(raw or b"{}")
            except Exception:
                return {}
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

    def __init__(self, *, ingress: str, admin: str, listen_port: int) -> None:
        self.ingress = ingress.rstrip("/")
        self.admin = admin.rstrip("/")
        self.listen_port = listen_port
        self._thread: threading.Thread | None = None
        self._shutdown: threading.Event | None = None
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
        """POST the workflow run handler keyed by task id — one execution
        per key, so duplicate submissions are naturally idempotent."""
        try:
            task_input = dict(task_input or {})
            task_id = str(
                task_input.get("id")
                or (task_input.get("state") or {}).get("id")
                or ""
            )
            if not task_id:
                import uuid
                task_id = uuid.uuid4().hex
                task_input["id"] = task_id
            resp = await asyncio.to_thread(
                _post, f"{self.ingress}/AgentTask/{task_id}/run", task_input
            )
            return task_id if resp is not None else None
        except Exception as e:
            log.warning("restate submit failed: %s", e)
            return None

    async def signal(self, task_id: str, action: str) -> bool:
        if action not in ("pause", "resume", "cancel"):
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
