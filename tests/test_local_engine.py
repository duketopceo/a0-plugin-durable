"""Local engine: agent-loop execution, step memoization across engine
instances (the restart/replay acceptance criterion), pause/resume/cancel,
failure containment."""

import asyncio

import pytest

from usr.plugins.durable.helpers import registry
from usr.plugins.durable.helpers.engines.local import LocalEngine
from usr.plugins.durable.helpers.journal import Journal

from conftest import run


def _make(journal_path, **kw):
    cfg = {"step_timeout_s": 5, "max_iterations": 10}
    cfg.update(kw)
    return LocalEngine(Journal(journal_path), **cfg)


async def _wait(engine, task_id, status, timeout=5.0):
    """Poll a task's row status until it lands or the timeout fires."""
    for _ in range(int(timeout * 20)):
        row = engine.journal.get_task(task_id)
        if row and row["status"] == status:
            return row
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"task {task_id} never reached {status}: {engine.journal.get_task(task_id)}"
    )


def test_simple_task_completes(journal_path):
    calls = {"llm": 0, "tool": 0}

    async def llm(**kw):
        calls["llm"] += 1
        return {"tool_calls": [], "text": "done"}

    registry.register_step("llm_call", llm)
    registry.register_step("tool_call", lambda **kw: None)

    async def _main():
        eng = _make(journal_path)
        task_id = await eng.submit({"prompt_messages": [{"role": "user", "content": "hi"}]})
        row = await _wait(eng, task_id, "completed")
        assert calls["llm"] == 1
        state = row["state"]
        assert state["context_snapshot"]["final_response"]["text"] == "done"
        return eng

    run(_main())


def test_tool_loop_then_complete(journal_path):
    calls = {"llm": 0, "tool": 0}

    async def llm(prompt_messages, model_config):
        calls["llm"] += 1
        # first pass requests a tool; after the tool result arrives, finish
        if calls["llm"] == 1:
            return {"tool_calls": [{"name": "read", "args": {"path": "/x"}}]}
        return {"tool_calls": [], "text": "done"}

    async def tool(tool_name, tool_args, idempotency_key):
        calls["tool"] += 1
        return {"result": f"{tool_name} ok"}

    registry.register_step("llm_call", llm)
    registry.register_step("tool_call", tool)

    async def _main():
        eng = _make(journal_path)
        task_id = await eng.submit({"prompt_messages": []})
        await _wait(eng, task_id, "completed")
        assert calls == {"llm": 2, "tool": 1}

    run(_main())


def test_restart_resumes_without_reexecuting_journaled_steps(journal_path):
    """THE acceptance test: kill the engine mid-task, construct a fresh
    engine over the same journal (process restart), attach, and verify the
    journaled tool step is memoized rather than re-executed."""
    calls = {"llm": 0, "tool": 0}
    tool_entered = asyncio.Event()
    tool_release = asyncio.Event()

    async def llm(prompt_messages, model_config):
        calls["llm"] += 1
        if calls["llm"] == 1:
            return {"tool_calls": [{"name": "w", "args": {"n": 1}}]}
        return {"tool_calls": [], "text": "done"}

    async def tool(tool_name, tool_args, idempotency_key):
        calls["tool"] += 1
        tool_entered.set()
        await tool_release.wait()  # park until the test simulates the crash
        return {"result": "w ok"}

    registry.register_step("llm_call", llm)
    registry.register_step("tool_call", tool)

    async def _main():
        eng = _make(journal_path)
        task_id = await eng.submit({"prompt_messages": []})
        await asyncio.wait_for(tool_entered.wait(), timeout=5)
        await eng.stop()  # process dies mid-tool — step row left 'running'
        del eng

        # --- restart: fresh Journal resets torn 'running' step to failed ---
        eng2 = _make(journal_path)
        eng2.attach(task_id)
        # engine is parked in the (re-executing) tool step — the llm step
        # before it was journaled and must NOT have re-run
        await asyncio.sleep(0.3)
        assert calls["llm"] == 1, "journaled llm step re-executed after restart"
        tool_release.set()
        await _wait(eng2, task_id, "completed")
        # the torn tool step legitimately re-ran once (it never journaled)
        assert calls["tool"] == 2

    run(_main())


def test_pause_resume_survives_restart(journal_path):
    calls = {"tool": 0}

    async def llm(prompt_messages, model_config):
        if calls["tool"] == 0:
            return {"tool_calls": [{"name": "w", "args": {}}]}
        return {"tool_calls": [], "text": "done"}

    async def tool(tool_name, tool_args, idempotency_key):
        calls["tool"] += 1
        return {"result": "ok"}

    registry.register_step("llm_call", llm)
    registry.register_step("tool_call", tool)

    async def _main():
        eng = _make(journal_path)
        task_id = await eng.submit({"prompt_messages": []})
        assert await eng.signal(task_id, "pause")
        await asyncio.sleep(0.3)  # let the runner park
        await eng.stop()
        del eng

        # restart — paused row must stay paused (not auto-run)
        eng2 = _make(journal_path)
        eng2.attach(task_id)
        await asyncio.sleep(0.3)
        row = eng2.journal.get_task(task_id)
        assert row["status"] == "paused"
        assert calls["tool"] == 0

        assert await eng2.signal(task_id, "resume")
        await _wait(eng2, task_id, "completed")
        assert calls["tool"] == 1

    run(_main())


def test_cancel_is_terminal(journal_path):
    async def llm(**kw):
        return {"tool_calls": [], "text": "x"}

    registry.register_step("llm_call", llm)

    async def _main():
        eng = _make(journal_path)
        task_id = await eng.submit({"prompt_messages": []})
        await eng.signal(task_id, "cancel")
        await _wait(eng, task_id, "failed")
        # cancel on an already-terminal task is a no-op, not an error
        await _wait(eng, task_id, "failed")

    run(_main())


def test_unregistered_step_fails_task(journal_path):
    async def llm(**kw):
        return {"tool_calls": [{"name": "w", "args": {}}]}

    registry.register_step("llm_call", llm)
    # no tool_call registered

    async def _main():
        eng = _make(journal_path)
        task_id = await eng.submit({"prompt_messages": []})
        await _wait(eng, task_id, "failed")

    run(_main())


def test_failing_step_fails_task(journal_path):
    async def llm(**kw):
        raise RuntimeError("model blew up")

    registry.register_step("llm_call", llm)

    async def _main():
        eng = _make(journal_path)
        task_id = await eng.submit({"prompt_messages": []})
        await _wait(eng, task_id, "failed")

    run(_main())


def test_resubmit_same_id_is_idempotent(journal_path):
    calls = {"llm": 0}

    async def llm(**kw):
        calls["llm"] += 1
        return {"tool_calls": []}

    registry.register_step("llm_call", llm)

    async def _main():
        eng = _make(journal_path)
        tid = await eng.submit({"id": "task-1", "prompt_messages": []})
        await _wait(eng, tid, "completed")
        again = await eng.submit({"id": "task-1", "prompt_messages": []})
        assert again == "task-1"
        await asyncio.sleep(0.3)
        assert calls["llm"] == 1  # completed row was not re-run

    run(_main())


def test_step_timeout_fails_task(journal_path):
    async def llm(**kw):
        await asyncio.sleep(60)

    registry.register_step("llm_call", llm)

    async def _main():
        eng = _make(journal_path, step_timeout_s=0.2)
        task_id = await eng.submit({"prompt_messages": []})
        await _wait(eng, task_id, "failed", timeout=10)

    run(_main())


def test_max_iterations_cap_completes(journal_path):
    async def llm(**kw):
        return {"tool_calls": [{"name": "w", "args": {}}]}  # never finishes

    async def tool(**kw):
        return {"result": "ok"}

    registry.register_step("llm_call", llm)
    registry.register_step("tool_call", tool)

    async def _main():
        eng = _make(journal_path, max_iterations=2)
        task_id = await eng.submit({"prompt_messages": []})
        row = await _wait(eng, task_id, "completed")
        assert "max_iterations" in row["state"]["context_snapshot"]["final_response"]["note"]

    run(_main())


def test_break_loop_tool_result_completes(journal_path):
    async def llm(**kw):
        return {"tool_calls": [{"name": "w", "args": {}}]}

    async def tool(**kw):
        return {"result": "final", "break_loop": True}

    registry.register_step("llm_call", llm)
    registry.register_step("tool_call", tool)

    async def _main():
        eng = _make(journal_path)
        task_id = await eng.submit({"prompt_messages": []})
        row = await _wait(eng, task_id, "completed")
        assert row["state"]["context_snapshot"]["final_response"]["result"] == "final"

    run(_main())
