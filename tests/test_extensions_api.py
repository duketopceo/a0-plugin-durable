"""Extension + api-handler surface: never-raise guarantees, job_loop tick,
startup configure, api contracts."""

import asyncio

from usr.plugins.durable.helpers import registry, runtime

from conftest import run


def test_job_loop_extension_ticks_runtime(local_cfg, monkeypatch):
    from usr.plugins.durable.extensions.python.job_loop._60_durable_reconcile import (
        DurableReconcile,
    )

    ticks = []
    monkeypatch.setattr(runtime, "tick", lambda: ticks.append(1) or asyncio.sleep(0))
    run(DurableReconcile().execute())
    assert ticks == [1]


def test_job_loop_extension_swallows_tick_failure(monkeypatch):
    from usr.plugins.durable.extensions.python.job_loop._60_durable_reconcile import (
        DurableReconcile,
    )

    async def boom():
        raise RuntimeError("engine exploded")

    monkeypatch.setattr(runtime, "tick", boom)
    run(DurableReconcile().execute())  # must not raise


def test_startup_extension_configures(local_cfg, monkeypatch):
    from usr.plugins.durable.extensions.python.startup_migration._60_durable_init import (
        DurableInit,
    )

    monkeypatch.setattr(
        "usr.plugins.durable.helpers.config.get_config", lambda: local_cfg
    )
    DurableInit().execute()
    assert runtime.is_active()
    assert runtime.engine_kind() == "local"


def test_startup_extension_never_raises(monkeypatch):
    from usr.plugins.durable.extensions.python.startup_migration._60_durable_init import (
        DurableInit,
    )

    monkeypatch.setattr(runtime, "configure", lambda *a, **kw: 1 / 0)
    DurableInit().execute()  # must not raise


def test_api_submit(local_cfg):
    from usr.plugins.durable.api.durable_submit import DurableSubmit

    async def llm(**kw):
        return {"tool_calls": []}

    registry.register_step("llm_call", llm)

    async def _main():
        runtime.configure(local_cfg)
        out = await DurableSubmit().process({"input": {"prompt_messages": []}}, None)
        assert out["ok"] and out["task_id"]

    run(_main())


def test_api_submit_inactive():
    from usr.plugins.durable.api.durable_submit import DurableSubmit

    async def _main():
        out = await DurableSubmit().process({"input": {}}, None)
        assert out["ok"] is False
        assert "inactive" in out["error"]

    run(_main())


def test_api_status_and_signal(local_cfg):
    from usr.plugins.durable.api.durable_signal import DurableSignal
    from usr.plugins.durable.api.durable_status import DurableStatus

    async def llm(**kw):
        return {"tool_calls": [], "text": "x"}

    registry.register_step("llm_call", llm)

    async def _main():
        runtime.configure(local_cfg)
        task_id = await runtime.submit({"prompt_messages": []})
        assert task_id
        missing = await DurableStatus().process({}, None)
        assert missing["ok"] is False
        bad = await DurableSignal().process({"task_id": task_id, "action": "explode"}, None)
        assert bad["ok"] is False
        # let the task finish, then status reports terminal state
        for _ in range(100):
            st = await runtime.status(task_id)
            if st and st.get("status") == "completed":
                break
            await asyncio.sleep(0.05)
        ok = await DurableStatus().process({"task_id": task_id}, None)
        assert ok["ok"] and ok["state"]["status"] == "completed"

    run(_main())


def test_api_handlers_require_auth():
    from usr.plugins.durable.api.durable_signal import DurableSignal
    from usr.plugins.durable.api.durable_status import DurableStatus
    from usr.plugins.durable.api.durable_submit import DurableSubmit

    for cls in (DurableSubmit, DurableStatus, DurableSignal):
        # a0's ApiHandler defaults both to True — pin that no handler weakens
        # them (the test stub returns True unconditionally, so the __dict__
        # check is what actually guards the contract)
        assert cls.requires_auth() is True
        assert cls.requires_csrf() is True
        assert "requires_auth" not in cls.__dict__
        assert "requires_csrf" not in cls.__dict__
        assert cls.get_methods() == ["POST"]


def test_api_non_dict_bodies_rejected(local_cfg):
    """A list/str/None body must return ok:false, not crash on .get()."""
    from usr.plugins.durable.api.durable_signal import DurableSignal
    from usr.plugins.durable.api.durable_status import DurableStatus
    from usr.plugins.durable.api.durable_submit import DurableSubmit

    async def _main():
        runtime.configure(local_cfg)
        for handler in (DurableSubmit(), DurableStatus(), DurableSignal()):
            for body in (None, [], "x", 42):
                out = await handler.process(body, None)
                assert out["ok"] is False, (type(handler).__name__, body, out)

    run(_main())


def test_api_signal_roundtrip_and_full_flag(local_cfg):
    """End-to-end: submit -> signal pause -> status reports paused; the
    'full' flag switches between bounded meta and the serialized state."""
    from usr.plugins.durable.api.durable_signal import DurableSignal
    from usr.plugins.durable.api.durable_status import DurableStatus
    from usr.plugins.durable.api.durable_submit import DurableSubmit

    async def llm(**kw):
        await asyncio.sleep(60)  # park mid-step so pause lands cleanly

    registry.register_step("llm_call", llm)

    async def _main():
        runtime.configure(local_cfg)
        out = await DurableSubmit().process({"input": {"prompt_messages": []}}, None)
        assert out["ok"]
        tid = out["task_id"]
        sig = await DurableSignal().process(
            {"task_id": tid, "action": "pause"}, None
        )
        assert sig == {"ok": True}
        bounded = await DurableStatus().process({"task_id": tid}, None)
        assert bounded["ok"] and bounded["state"]["status"] == "paused"
        assert "context_snapshot" not in bounded["state"]  # bounded meta
        full = await DurableStatus().process({"task_id": tid, "full": True}, None)
        assert full["ok"] and "context_snapshot" in full["state"]
        # cancel -> terminal
        sig2 = await DurableSignal().process(
            {"task_id": tid, "action": "cancel"}, None
        )
        assert sig2 == {"ok": True}
        for _ in range(100):
            st = await runtime.status(tid)
            if st and st.get("status") == "failed":
                break
            await asyncio.sleep(0.05)
        assert (await runtime.status(tid))["status"] == "failed"

    run(_main())


def test_hooks_install_uninstall(local_cfg, monkeypatch):
    """install() is log-only (startup_migration owns configure); uninstall()
    tears the engine down and is idempotent."""
    monkeypatch.setattr(
        "usr.plugins.durable.helpers.config.get_config", lambda: local_cfg
    )
    import usr.plugins.durable.hooks as hooks

    hooks.install()
    assert runtime.is_active() is False  # configure happens in the extension
    runtime.configure(local_cfg)
    assert runtime.is_active()
    hooks.uninstall()
    assert runtime.is_active() is False
    hooks.uninstall()  # idempotent


def test_uninstall_stops_owned_services(local_cfg):
    """uninstall() acceptance: plugin-owned services stop; journal survives."""
    async def llm(**kw):
        await asyncio.sleep(60)

    registry.register_step("llm_call", llm)

    async def _main():
        runtime.configure(local_cfg)
        await runtime.submit({"prompt_messages": []})
        engine = runtime._engine
        assert engine._runners
        import usr.plugins.durable.hooks as hooks
        hooks.uninstall()  # the real a0 unload path — engine stops via _reset
        assert runtime.is_active() is False
        assert runtime._engine is None
        await asyncio.sleep(0.1)  # let the scheduled teardown settle
        # journal persisted — a reinstall over the same path sees the task
        from usr.plugins.durable.helpers.journal import Journal
        row = Journal(local_cfg["journal_path"]).incomplete_tasks()
        assert len(row) == 1

    run(_main())
