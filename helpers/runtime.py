"""Runtime facade — engine selection + the never-raise public surface.

configure() builds the engine synchronously; the engine's serving side
starts lazily on first async touch via the uniform ``start()`` contract
(startup_migration is SYNC — there is no event loop at plugin init).
configure() refuses to replace a live engine: silent replacement would
orphan runners double-executing on the same journal — call shutdown()
first to reconfigure.

Public surface: submit/status/signal/tick/shutdown. Every entry point is
failure-contained — a durable-exec misconfiguration or a dead Restate
server must never take a0 down. ``is_active()`` means 'engine armed', not
necessarily 'serving' (a restate endpoint comes up lazily).
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
_cfg_dynamic = False  # True when _cfg came from get_config() (host settings)
_lock = threading.Lock()
# Held refs to in-flight teardown tasks — fire-and-forget tasks are GC-unsafe.
_pending_stops: set[asyncio.Task] = set()


def _plugin_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _default_journal_path() -> str:
    return str(_plugin_root() / "data" / "durable.sqlite3")


def configure(cfg: dict[str, Any] | None = None) -> bool:
    """Select + construct the engine. Returns True when an engine is armed.
    Never raises — returns False on any failure and the plugin goes inert.
    Refuses to replace a live engine (orphaned runners would double-execute
    against the shared journal): shutdown() before reconfiguring."""
    global _engine, _engine_kind, _engine_started, _cfg, _cfg_dynamic
    with _lock:
        if _engine is not None:
            log.warning(
                "durable: configure() called while engine %r is live — "
                "keeping existing config (shutdown() first to reconfigure)",
                _engine_kind,
            )
            return True
        _engine_kind = ""
        _engine_started = False
        try:
            if isinstance(cfg, dict):
                _cfg = cfg
                _cfg_dynamic = False  # programmatic config — not re-read live
            else:
                _cfg = _cfg_mod.get_config()
                _cfg_dynamic = True   # host config — tick() re-reads it so a
                                      # mid-process `enabled: false` shuts down
        except Exception:
            _cfg = dict(_cfg_mod.DEFAULTS)
            _cfg_dynamic = True
        if not _cfg_mod.truthy(_cfg.get("enabled")):
            return False
        try:
            registry.register_defaults()
            kind = str(_cfg.get("engine", "local")).strip().lower()
            if kind == "local":
                from usr.plugins.durable.helpers.journal import Journal
                from usr.plugins.durable.helpers.engines.local import LocalEngine

                path = os.path.expanduser(
                    str(_cfg.get("journal_path") or _default_journal_path())
                )
                resolved = Path(path).resolve()
                if not str(resolved).startswith(str(_plugin_root().resolve())):
                    log.warning(
                        "durable: journal_path %s resolves outside the plugin "
                        "dir — WAL needs a local filesystem and exactly one "
                        "writer process", resolved,
                    )
                journal = Journal(resolved)
                _engine = LocalEngine(
                    journal,
                    step_timeout_s=_cfg_mod.num(_cfg.get("step_timeout_s"), 300),
                    max_iterations=int(_cfg_mod.num(_cfg.get("max_iterations"), 100)),
                    max_concurrent=int(_cfg_mod.num(_cfg.get("max_concurrent"), 8)),
                )
                _engine_kind = "local"
                return True
            if kind == "restate":
                from usr.plugins.durable.helpers.engines.restate import RestateEngine

                engine = RestateEngine(
                    ingress=str(_cfg.get("restate_ingress", "")),
                    admin=str(_cfg.get("restate_admin", "")),
                    listen_port=int(_cfg_mod.num(_cfg.get("listen_port"), 9080)),
                    # step_timeout_s is a local-engine knob — Restate owns
                    # step timeouts/retries via its own retry policy
                    defaults={
                        "max_iterations": int(_cfg_mod.num(_cfg.get("max_iterations"), 100)),
                    },
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


_start_task: asyncio.Task | None = None


async def _ensure_started() -> bool:
    """Drive the engine's uniform async start() once — concurrent callers
    share one start task so e.g. the restate ASGI port can't double-bind.
    Engines needing no start just return True. Never raises."""
    global _engine_started, _start_task
    if _engine is None:
        return False
    if _engine_started:
        return True
    try:
        if _start_task is None or _start_task.done():
            _start_task = asyncio.get_running_loop().create_task(_engine.start())
        _engine_started = bool(await _start_task)
        return _engine_started
    except Exception as e:
        log.warning("durable: engine start failed: %s", e)
        return False


def is_active() -> bool:
    """Engine armed. For lazy-start engines (restate) 'armed' precedes
    'serving' — submit/status/signal drive the start themselves."""
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


async def meta(task_id: str) -> dict[str, Any] | None:
    """Bounded poll projection (id/status/timestamps) — the default for the
    status endpoint; pass ``full`` to get the serialized TaskState."""
    try:
        if not await _ensure_started():
            return None
        fn = getattr(_engine, "meta", None) or _engine.status
        return await fn(task_id)
    except Exception as e:
        log.warning("durable meta(%s) failed: %s", task_id, e)
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

    1. Re-read config; a mid-process disable shuts the live engine down.
    2. Submit each enabled config task (engines dedupe by task id). The
       ``input`` wrapper unwraps — same shape as the API's {"input": {...}}.
    3. Local engine only: re-attach runners for non-terminal tasks so work
       resumes after a process restart (restate replays server-side).
    Never raises — a bad tick must not stall the host's job loop.
    """
    try:
        if (
            _engine is not None
            and _cfg_dynamic
            and not _cfg_mod.truthy(_cfg_mod.get_config().get("enabled"))
        ):
            # host disabled the plugin mid-process — tear the engine down
            await shutdown()
            return
        if not await _ensure_started():
            return
        for task in _cfg.get("tasks") or []:
            try:
                if not isinstance(task, dict) or not _cfg_mod.truthy(task.get("enabled", True)):
                    continue
                task_id = str(task.get("id") or "").strip()
                if not task_id:
                    continue
                # documented shape: {id, enabled, input: {...}} — the payload
                # is 'input' (same as the API's unwrap), everything else stays
                # out of the journaled task input
                payload = dict(task.get("input") or {})
                for k, v in task.items():
                    if k not in ("id", "enabled", "input"):
                        payload.setdefault(k, v)
                payload["id"] = task_id
                await submit(payload)
            except Exception as e:
                log.warning("durable tick: task submit failed: %s", e)
        # re-attach non-terminal tasks — a no-op on the restate engine (the
        # server replays independently of a0's process)
        _engine.resume_incomplete()
    except Exception as e:
        log.warning("durable tick failed: %s", e)


async def shutdown() -> None:
    """Stop plugin-owned services (restate ASGI endpoint, local runners).
    Journaled state survives — tasks resume on next configure+tick.
    Registry entries are host-owned and stay."""
    global _engine, _engine_kind, _engine_started, _cfg, _cfg_dynamic, _start_task
    try:
        if _engine is not None:
            await _engine.stop()
    except Exception as e:
        log.warning("durable shutdown failed: %s", e)
    finally:
        _engine = None
        _engine_kind = ""
        _engine_started = False
        _start_task = None
        _cfg = {}
        _cfg_dynamic = False


def _reset() -> None:
    """Full teardown: engine stop THEN registry reset — clearing the registry
    while runners live would turn their next step into a bogus
    'no step registered' failure instead of a resumable interrupt. Called by
    tests and by hooks.uninstall() (a0's unload path)."""
    global _engine, _engine_kind, _engine_started, _cfg, _cfg_dynamic, _start_task
    engine = _engine
    _engine = None
    _engine_kind = ""
    _engine_started = False
    _start_task = None
    _cfg = {}
    _cfg_dynamic = False
    if engine is not None:
        async def _teardown() -> None:
            try:
                await engine.stop()
            except Exception:
                pass
            registry.reset()

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                asyncio.run(_teardown())
            except Exception:
                registry.reset()
        else:
            task = loop.create_task(_teardown())
            _pending_stops.add(task)
            task.add_done_callback(_pending_stops.discard)
            return
    registry.reset()
