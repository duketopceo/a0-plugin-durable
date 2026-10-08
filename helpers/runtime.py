"""Runtime facade — engine selection + the never-raise public surface.

configure() builds the engine synchronously; the Restate engine's serving
side starts lazily on first async touch (submit/tick) because a0's
startup_migration extensions run SYNC — there is no event loop at plugin
init. Local engine needs no start step.

Public surface: submit/status/signal/tick/shutdown. Every entry point is
failure-contained — a durable-exec misconfiguration or a dead Restate
server must never take a0 down.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
from pathlib import Path
from typing import Any

from usr.plugins.durable.helpers import LOG_NAME
from usr.plugins.durable.helpers import config as _cfg_mod
from usr.plugins.durable.helpers import registry

log = logging.getLogger(LOG_NAME)

_engine: Any | None = None
_engine_kind = ""
_engine_started = False
_cfg: dict[str, Any] = {}
_lock = threading.Lock()


def _plugin_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _default_journal_path() -> str:
    return str(_plugin_root() / "data" / "durable.sqlite3")


def configure(cfg: dict[str, Any] | None = None) -> bool:
    """Select + construct the engine. Returns True when an engine is armed.
    Never raises — returns False on any failure and the plugin goes inert."""
    global _engine, _engine_kind, _engine_started, _cfg
    with _lock:
        _engine = None
        _engine_kind = ""
        _engine_started = False
        _cfg = cfg or _cfg_mod.get_config()
        if not _cfg_mod.truthy(_cfg.get("enabled")):
            return False
        try:
            registry.register_defaults()
            kind = str(_cfg.get("engine", "local")).strip().lower()
            if kind == "local":
                from usr.plugins.durable.helpers.journal import Journal
                from usr.plugins.durable.helpers.engines.local import LocalEngine

                path = os.path.expanduser(str(_cfg.get("journal_path") or _default_journal_path()))
                journal = Journal(path)
                _engine = LocalEngine(
                    journal,
                    step_timeout_s=_cfg_mod.num(_cfg.get("step_timeout_s"), 300),
                    max_iterations=int(_cfg_mod.num(_cfg.get("max_iterations"), 100)),
                )
                _engine_kind = "local"
                _engine_started = True
                return True
            if kind == "restate":
                from usr.plugins.durable.helpers.engines.restate import RestateEngine

                engine = RestateEngine(
                    ingress=str(_cfg.get("restate_ingress", "")),
                    admin=str(_cfg.get("restate_admin", "")),
                    listen_port=int(_cfg_mod.num(_cfg.get("listen_port"), 9080)),
                )
                if not engine.available:
                    log.warning(
                        "durable: engine=restate but restate_sdk/hypercorn not "
                        "installed — plugin inert (pip install restate_sdk hypercorn)"
                    )
                    return False
                _engine = engine
                _engine_kind = "restate"
                # _engine_started flips on first async _ensure_started()
                return True
            log.warning("durable: unknown engine %r — plugin inert", kind)
            return False
        except Exception as e:
            log.warning("durable: configure failed: %s", e)
            _engine = None
            return False


async def _ensure_started() -> bool:
    """Start the engine's serving side on first async use (restate only;
    local is already started). Never raises."""
    global _engine_started
    if _engine is None:
        return False
    if _engine_started:
        return True
    try:
        if _engine_kind == "restate":
            _engine_started = await _engine.start()
        else:
            _engine_started = True
        return _engine_started
    except Exception as e:
        log.warning("durable: engine start failed: %s", e)
        return False


def is_active() -> bool:
    return _engine is not None


def engine_kind() -> str:
    return _engine_kind


async def submit(task_input: dict[str, Any]) -> str | None:
    """Submit a durable task; returns the task id or None."""
    try:
        if not await _ensure_started():
            return None
        return await _engine.submit(task_input)
    except Exception as e:
        log.warning("durable submit failed: %s", e)
        return None


async def status(task_id: str) -> dict[str, Any] | None:
    try:
        if not await _ensure_started():
            return None
        return await _engine.status(task_id)
    except Exception as e:
        log.warning("durable status(%s) failed: %s", task_id, e)
        return None


async def signal(task_id: str, action: str) -> bool:
    try:
        if not await _ensure_started():
            return False
        return bool(await _engine.signal(task_id, action))
    except Exception as e:
        log.warning("durable signal(%s,%s) failed: %s", task_id, action, e)
        return False


async def tick() -> None:
    """job_loop reconcile — idempotent, safe every loop:

    1. Submit each enabled config task (engines dedupe by task id).
    2. Local engine only: re-attach runners for non-terminal tasks so work
       resumes after a process restart (restate replays server-side).
    Never raises — a bad tick must not stall the host's job loop.
    """
    try:
        if not await _ensure_started():
            return
        for task in _cfg.get("tasks") or []:
            try:
                if not isinstance(task, dict) or not _cfg_mod.truthy(task.get("enabled", True)):
                    continue
                task_id = str(task.get("id") or "").strip()
                if not task_id:
                    continue
                await submit({**task, "id": task_id})
            except Exception as e:
                log.warning("durable tick: task submit failed: %s", e)
        journal = getattr(_engine, "journal", None)
        attach = getattr(_engine, "attach", None)
        if journal is not None and attach is not None:
            for task_id in journal.incomplete_tasks():
                attach(task_id)
    except Exception as e:
        log.warning("durable tick failed: %s", e)


async def shutdown() -> None:
    """Stop plugin-owned services (restate ASGI endpoint, local runners).
    Journaled state survives — tasks resume on next configure+tick."""
    global _engine, _engine_kind, _engine_started
    try:
        if _engine is not None:
            stop = getattr(_engine, "stop", None)
            if stop is not None:
                await stop()
    except Exception as e:
        log.warning("durable shutdown failed: %s", e)
    finally:
        _engine = None
        _engine_kind = ""
        _engine_started = False


def _reset() -> None:
    """Test/reload hook — teardown outside a running loop."""
    global _engine, _engine_kind, _engine_started
    try:
        engine = _engine
        if engine is not None:
            stop = getattr(engine, "stop", None)
            if stop is not None:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    asyncio.run(stop())
                else:
                    loop.create_task(stop())
    except Exception:
        pass
    _engine = None
    _engine_kind = ""
    _engine_started = False
    _cfg = {}
    registry.reset()
