"""Local engine: agent-loop execution, step memoization across engine
instances (the restart/replay acceptance criterion), pause/resume/cancel,
failure containment."""

import asyncio

import pytest

from usr.plugins.durable.helpers import registry
from usr.plugins.durable.helpers.contract import TaskStatus
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


def test_submit_rejects_invalid_ids(journal_path):
    async def _main():
        eng = _make(journal_path)
        for bad in ("../escape", "has space", "x" * 200):
            assert await eng.submit({"id": bad, "prompt_messages": []}) is None

    run(_main())


def test_submit_oversized_input_rejected(journal_path):
    async def _main():
        eng = _make(journal_path)
        assert await eng.submit({"blob": "x" * (1 << 20)}) is None

    run(_main())


def test_submit_dead_journal_returns_none(journal_path):
    """Phantom-task fix: a failed insert must not claim a task_id."""
    async def _main():
        eng = _make(journal_path)
        eng.journal.close()
        assert await eng.submit({"prompt_messages": []}) is None

    run(_main())


def test_signal_negatives(journal_path):
    async def _main():
        eng = _make(journal_path)
        assert await eng.signal("nope", "pause") is False       # unknown task
        eng.journal.create_task("live", {})
        assert await eng.signal("live", "explode") is False    # bogus action
        eng.journal.create_task("done", {})
        eng.journal.update_task("done", status=TaskStatus.COMPLETED)
        assert await eng.signal("done", "pause") is False      # terminal

    run(_main())


def test_paused_task_not_auto_attached_then_resume(journal_path):
    """Resume-on-demand: a paused row must NOT get a runner from
    resume_incomplete (ticks would accumulate parked runners); the explicit
    resume signal attaches one."""
    calls = {"llm": 0}

    async def llm(**kw):
        calls["llm"] += 1
        return {"tool_calls": [], "text": "done"}

    registry.register_step("llm_call", llm)

    async def _main():
        eng = _make(journal_path)
        eng.journal.create_task("t", {"prompt_messages": []})
        eng.journal.update_task("t", status=TaskStatus.PAUSED)
        eng.resume_incomplete()          # paused -> skipped
        await asyncio.sleep(0.2)
        assert calls["llm"] == 0 and not eng._runners
        assert await eng.signal("t", "resume") is True
        await _wait(eng, "t", "completed")
        assert calls["llm"] == 1

    run(_main())


def test_cancel_while_parked_unsticks_and_stays_terminal(journal_path):
    """A cancel landing on a parked runner must end the task — the wake path
    used to flip 'failed' back to 'executing'."""
    async def llm(**kw):
        await asyncio.sleep(60)

    registry.register_step("llm_call", llm)

    async def _main():
        eng = _make(journal_path)
        task_id = await eng.submit({"prompt_messages": []})
        assert await eng.signal(task_id, "pause")
        await asyncio.sleep(0.2)          # runner parks in _await_if_paused
        assert await eng.signal(task_id, "cancel")
        await _wait(eng, task_id, "failed")
        await asyncio.sleep(0.2)          # parked runner wakes, exits —
        assert eng.journal.get_status(task_id) == "failed"  # no clobber-back
        runner = eng._runners.get(task_id)
        assert runner is None or runner.done()

    run(_main())


def test_non_dict_step_result_fails_task(journal_path):
    async def llm(**kw):
        return ["not", "a", "dict"]

    registry.register_step("llm_call", llm)

    async def _main():
        eng = _make(journal_path)
        task_id = await eng.submit({"prompt_messages": []})
        await _wait(eng, task_id, "failed")

    run(_main())


def test_non_serializable_step_result_fails_not_retries(journal_path):
    """A result that can't JSON is a failed step — it must not sit 'running'
    and re-execute the side effect on replay."""
    calls = {"llm": 0}

    async def llm(**kw):
        calls["llm"] += 1
        return {"cb": object()}  # JSON can't serialize this

    registry.register_step("llm_call", llm)

    async def _main():
        eng = _make(journal_path)
        task_id = await eng.submit({"prompt_messages": []})
        await _wait(eng, task_id, "failed")
        assert calls["llm"] == 1  # failed once — no silent retry
        assert eng.journal.step_result(task_id, "iter0:llm") is None

    run(_main())


def test_corrupt_state_fails_closed(journal_path):
    """A corrupt state_json must not run the task on fabricated state."""
    async def _main():
        eng = _make(journal_path)
        eng.journal.create_task("t", {"prompt_messages": []})
        with eng.journal._lock, eng.journal._con:
            eng.journal._con.execute(
                "UPDATE tasks SET state_json='{corrupt' WHERE id='t'"
            )
        eng.attach("t")
        await _wait(eng, "t", "failed")

    run(_main())


def test_task_level_max_iterations_override(journal_path):
    calls = {"llm": 0}

    async def llm(**kw):
        calls["llm"] += 1
        return {"tool_calls": [{"name": "w", "args": {}}]}

    async def tool(**kw):
        return {"result": "ok"}

    registry.register_step("llm_call", llm)
    registry.register_step("tool_call", tool)

    async def _main():
        eng = _make(journal_path, max_iterations=10)
        # per-task override tightens the loop
        tid = await eng.submit({"prompt_messages": [], "max_iterations": 1})
        row = await _wait(eng, tid, "completed")
        assert calls["llm"] == 1
        assert "max_iterations" in row["state"]["context_snapshot"]["final_response"]["note"]
        # zero iterations is legal — completes with the cap note, no llm call
        calls["llm"] = 0
        tid0 = await eng.submit({"prompt_messages": [], "max_iterations": 0})
        await _wait(eng, tid0, "completed")
        assert calls["llm"] == 0
        # an outrageous override is clamped, not honored
        tid2 = await eng.submit({"prompt_messages": [], "max_iterations": 10**9})
        await _wait(eng, tid2, "completed", timeout=30)
        assert calls["llm"] == 100  # clamp = engine default * 10

    run(_main())


def test_replay_does_not_duplicate_tool_messages(journal_path):
    """iter0 checkpoints its tool message inside prompt_messages; on restart
    the replayed iteration consumes the memo WITHOUT appending again."""
    seen: list[list] = []
    tool_runs = {"a": 0, "b": 0}
    park_b = {"armed": True}

    async def llm(prompt_messages, model_config):
        seen.append(list(prompt_messages))
        n = len(seen)
        if n == 1:
            return {"tool_calls": [{"name": "a", "args": {}}]}
        if n == 2:
            return {"tool_calls": [{"name": "b", "args": {}}]}
        return {"tool_calls": [], "text": "done"}

    async def tool(tool_name, tool_args, idempotency_key):
        tool_runs[tool_name] += 1
        if tool_name == "b" and park_b["armed"]:
            park_b["armed"] = False
            await asyncio.sleep(60)  # torn — engine dies inside the step
        return {"result": f"{tool_name}-ok"}

    registry.register_step("llm_call", llm)
    registry.register_step("tool_call", tool)

    async def _main():
        eng = _make(journal_path)
        task_id = await eng.submit({"prompt_messages": []})
        for _ in range(100):  # wait for tool b to be mid-step
            if tool_runs["b"]:
                break
            await asyncio.sleep(0.05)
        assert tool_runs["b"] == 1
        await eng.stop()  # process dies inside tool b (torn 'running' row)

        eng2 = _make(journal_path)  # restart — torn step resets to failed
        eng2.attach(task_id)
        await _wait(eng2, task_id, "completed")
        final = seen[-1]
        assert sum(1 for m in final if m.get("name") == "a") == 1
        assert sum(1 for m in final if m.get("name") == "b") == 1
        # 'a' memoized (journaled + checkpointed); torn 'b' legitimately re-ran
        assert tool_runs == {"a": 1, "b": 2}

    run(_main())


def test_runner_cap_defers_overflow(journal_path):
    """max_concurrent is a hard cap — excess tasks stay resumable in the
    journal instead of spawning unbounded asyncio work."""
    release = asyncio.Event()

    async def llm(**kw):
        await release.wait()
        return {"tool_calls": []}

    registry.register_step("llm_call", llm)

    async def _main():
        eng = _make(journal_path, max_concurrent=1)
        t1 = await eng.submit({"id": "t1", "prompt_messages": []})
        t2 = await eng.submit({"id": "t2", "prompt_messages": []})
        await asyncio.sleep(0.3)
        live = [k for k, t in eng._runners.items() if not t.done()]
        assert live == ["t1"]          # second task deferred, not dropped
        release.set()
        await _wait(eng, t1, "completed")
        # a finishing runner drains the backlog — no tick needed
        await _wait(eng, t2, "completed")

    run(_main())
