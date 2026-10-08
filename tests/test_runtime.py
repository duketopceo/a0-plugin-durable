"""Runtime facade: configure gating, engine selection, public surface,
job_loop tick semantics, shutdown."""

import asyncio

import pytest

from usr.plugins.durable.helpers import registry, runtime

from conftest import run


def test_inactive_when_disabled():
    assert runtime.configure({"enabled": False}) is False
    assert runtime.is_active() is False


def test_unknown_engine_inert():
    assert runtime.configure({"enabled": True, "engine": "temporal"}) is False
    assert runtime.is_active() is False


def test_local_engine_activates(local_cfg):
    assert runtime.configure(local_cfg) is True
    assert runtime.is_active()
    assert runtime.engine_kind() == "local"
    assert "api_call" in registry.registered_steps()  # built-in wired


def test_restate_engine_without_sdk_inert():
    cfg = {"enabled": True, "engine": "restate"}
    # restate_sdk/hypercorn aren't test deps — engine must degrade cleanly
    assert runtime.configure(cfg) is False
    assert runtime.is_active() is False


def test_submit_status_roundtrip(local_cfg):
    async def llm(**kw):
        return {"tool_calls": [], "text": "hi"}

    registry.register_step("llm_call", llm)

    async def _main():
        runtime.configure(local_cfg)
        task_id = await runtime.submit({"prompt_messages": []})
        assert task_id
        for _ in range(100):
            st = await runtime.status(task_id)
            if st and st.get("status") == "completed":
                break
            await asyncio.sleep(0.05)
        st = await runtime.status(task_id)
        assert st["status"] == "completed"

    run(_main())


def test_submit_inactive_returns_none():
    async def _main():
        assert await runtime.submit({"x": 1}) is None
        assert await runtime.status("t") is None
        assert await runtime.signal("t", "pause") is False

    run(_main())


def test_tick_submits_configured_tasks_idempotently(local_cfg):
    calls = {"llm": 0}

    async def llm(**kw):
        calls["llm"] += 1
        return {"tool_calls": []}

    registry.register_step("llm_call", llm)
    local_cfg["tasks"] = [{"id": "job-1", "enabled": True, "prompt_messages": []},
                          {"id": "job-off", "enabled": False, "prompt_messages": []}]

    async def _main():
        runtime.configure(local_cfg)
        await runtime.tick()
        await runtime.tick()  # second tick must not re-submit
        import asyncio
        await asyncio.sleep(0.5)
        st = await runtime.status("job-1")
        assert st is not None and st.get("status") == "completed"
        assert await runtime.status("job-off") is None
        assert calls["llm"] == 1

    run(_main())


def test_tick_resumes_incomplete_after_restart(local_cfg):
    """Simulate restart: task row exists mid-flight (tool step torn
    'running'), new runtime over the same journal; tick() must re-attach
    and finish it."""
    calls = {"llm": 0, "tool": 0}
    tool_started = []  # per-loop asyncio.Event (events can't cross asyncio.run)
    task_ids = []

    async def llm(**kw):
        calls["llm"] += 1
        # only the first llm invocation requests the tool; after the tool
        # result is appended the next call finishes the task
        if calls["llm"] == 1:
            return {"tool_calls": [{"name": "w", "args": {}}]}
        return {"tool_calls": [], "text": "done"}

    async def tool_marking(**kw):
        calls["tool"] += 1
        tool_started[0].set()
        await asyncio.sleep(60)  # parked until the shutdown kills it
        return {"result": "ok"}

    registry.register_step("llm_call", llm)
    registry.register_step("tool_call", tool_marking)

    async def _crash_mid_tool():
        tool_started.append(asyncio.Event())
        runtime.configure(local_cfg)
        task_ids.append(await runtime.submit({"prompt_messages": []}))
        # wait until the tool step is actually running, then kill the engine —
        # the step row is left torn 'running' in the journal
        await asyncio.wait_for(tool_started[0].wait(), timeout=5)
        await runtime.shutdown()

    run(_crash_mid_tool())

    # --- restart: fresh runtime over the same journal ----------------------
    async def tool_free(**kw):
        calls["tool"] += 1
        return {"result": "ok"}

    async def _resumed():
        registry.reset()
        registry.register_step("llm_call", llm)
        registry.register_step("tool_call", tool_free)
        runtime.configure(local_cfg)
        await runtime.tick()  # re-attaches the incomplete task
        task_id = task_ids[0]
        for _ in range(100):
            st = await runtime.status(task_id)
            if st and st.get("status") == "completed":
                break
            await asyncio.sleep(0.05)
        st = await runtime.status(task_id)
        assert st["status"] == "completed"
        # llm ran once pre-crash (journaled → memoized on replay) + once for
        # the post-tool pass; the torn tool step legitimately re-ran
        assert calls["llm"] == 2
        assert calls["tool"] == 2

    run(_resumed())


def test_shutdown_clears(local_cfg):
    async def _main():
        runtime.configure(local_cfg)
        await runtime.shutdown()
        assert runtime.is_active() is False

    run(_main())


def test_configure_never_raises():
    assert runtime.configure({"enabled": True, "engine": "local",
                              "journal_path": "/nonexistent-dir-x/../x.db"}) in (True, False)
    runtime._reset()
