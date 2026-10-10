"""SQLite step journal — the local engine's durable substrate.

Two tables:

- ``tasks``: one row per durable task — id, lifecycle status, the task input
  and the latest serialized ``TaskState``.
- ``steps``: one row per journaled step — ``(task_id, step_key)`` is the
  memoization key; a completed row IS the replay record.

One persistent connection is held for the journal's lifetime with
``check_same_thread=False``; every access is serialized through the lock.
That is the simplest correct shape for sqlite3 (a Connection is not
shareable unguarded, and per-call connections leak fds to refcount GC).
The control column is ``tasks.status`` — ``state_json`` is checkpoint
payload only. Corruption is contained: journal errors mark the task failed
or return None rather than propagating into the host.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from usr.plugins.durable.helpers import LOG_NAME
from usr.plugins.durable.helpers.contract import (
    TERMINAL_VALUES,
    TaskState,
    TaskStatus,
    json_or,
)

log = logging.getLogger(LOG_NAME)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id          TEXT PRIMARY KEY,
    status      TEXT NOT NULL,
    input_json  TEXT NOT NULL DEFAULT '{}',
    state_json  TEXT NOT NULL DEFAULT '{}',
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS steps (
    task_id     TEXT NOT NULL,
    step_key    TEXT NOT NULL,
    name        TEXT NOT NULL,
    status      TEXT NOT NULL,           -- running|done|failed
    result_json TEXT,
    error       TEXT,
    updated_at  REAL NOT NULL,
    PRIMARY KEY (task_id, step_key)
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_steps_status ON steps(status);
"""

# Statuses a runner may pick up. 'paused' is deliberately absent — a paused
# task stays parked until an explicit resume signal attaches a runner.
_RESUMABLE_VALUES = tuple(
    s.value
    for s in (
        TaskStatus.CREATED,
        TaskStatus.PLANNED,
        TaskStatus.EXECUTING,
        TaskStatus.CHECKPOINTED,
        TaskStatus.RESUMED,
    )
)


class Journal:
    """Durable task/step store. Every method is failure-contained: journal
    errors mark the task failed or return None — they never propagate into
    the host."""

    def __init__(self, path: str | Path) -> None:
        self._path = str(path)
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._con = sqlite3.connect(
            self._path, check_same_thread=False, timeout=30
        )
        self._con.execute("PRAGMA journal_mode=WAL")  # persistent db setting
        # FULL so a power loss can't drop the last committed step_done — the
        # 'journaled before trusted' contract depends on the write surviving.
        self._con.execute("PRAGMA synchronous=FULL")
        with self._lock, self._con:
            self._con.executescript(_SCHEMA)
            # A crash can strand 'running' steps — reset them to failed so a
            # resumed task re-executes rather than trusting a torn write.
            self._con.execute(
                "UPDATE steps SET status='failed', error='interrupted by restart' "
                "WHERE status='running'"
            )

    def close(self) -> None:
        try:
            self._con.close()
        except Exception:
            pass

    # --- tasks --------------------------------------------------------------

    def create_task(self, task_id: str, task_input: dict[str, Any]) -> bool:
        """Insert a CREATED task. Idempotent: existing rows are left alone
        and False is returned (so job_loop ticks can re-submit safely)."""
        try:
            with self._lock, self._con:
                cur = self._con.execute(
                    "INSERT OR IGNORE INTO tasks(id,status,input_json,state_json,"
                    "created_at,updated_at) VALUES (?,?,?,?,?,?)",
                    (
                        task_id,
                        TaskStatus.CREATED.value,
                        json.dumps(task_input),
                        TaskState(id=task_id).to_json(),
                        time.time(),
                        time.time(),
                    ),
                )
                return cur.rowcount > 0
        except Exception as e:
            log.warning("journal create_task(%s) failed: %s", task_id, e)
            return False

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        try:
            with self._lock:
                row = self._con.execute(
                    "SELECT status,input_json,state_json FROM tasks WHERE id=?",
                    (task_id,),
                ).fetchone()
            if row is None:
                return None
            input_parsed = json_or(row[1], {})
            state_parsed = json_or(row[2], None)
            return {
                "status": row[0],
                "input": input_parsed if isinstance(input_parsed, dict) else {},
                # corrupt/non-dict state_json surfaces as None — the engine
                # fails the task closed instead of running on fabricated state
                "state": state_parsed if isinstance(state_parsed, dict) else None,
            }
        except Exception as e:
            log.warning("journal get_task(%s) failed: %s", task_id, e)
            return None

    def get_status(self, task_id: str) -> str | None:
        """Status column only — the control-plane read. state_json grows with
        history, so hot paths (pause/halt polls) must not parse it."""
        try:
            with self._lock:
                row = self._con.execute(
                    "SELECT status FROM tasks WHERE id=?", (task_id,)
                ).fetchone()
            return row[0] if row else None
        except Exception as e:
            log.warning("journal get_status(%s) failed: %s", task_id, e)
            return None

    def update_task(
        self,
        task_id: str,
        *,
        status: TaskStatus | None = None,
        state: TaskState | None = None,
        non_terminal_only: bool = False,
    ) -> bool:
        """Returns True when the write committed. ``non_terminal_only`` makes
        the status write a compare-and-swap — a signal/terminal write that
        already landed wins over a stale runner write (and vice versa)."""
        try:
            # Serialize BEFORE the transaction: a to_json() failure must not
            # roll back a status write that already committed in the same txn.
            state_json = state.to_json() if state is not None else None
        except Exception as e:
            log.warning("journal update_task(%s): state not serializable: %s", task_id, e)
            return False
        try:
            with self._lock, self._con:
                if status is not None:
                    guard = (
                        " AND status NOT IN ('completed','failed')"
                        if non_terminal_only
                        else ""
                    )
                    cur = self._con.execute(
                        f"UPDATE tasks SET status=?, updated_at=? WHERE id=?{guard}",
                        (status.value, time.time(), task_id),
                    )
                    if non_terminal_only and cur.rowcount == 0:
                        # CAS lost — a terminal status landed (or the task is
                        # gone); the caller must not act as if its write won
                        return False
                if state_json is not None:
                    self._con.execute(
                        "UPDATE tasks SET state_json=?, updated_at=? WHERE id=?",
                        (state_json, time.time(), task_id),
                    )
            return True
        except Exception as e:
            log.warning("journal update_task(%s) failed: %s", task_id, e)
            return False

    def get_meta(self, task_id: str) -> dict[str, Any] | None:
        """Bounded status projection — hot poll path; never parses
        state_json (which grows with task history)."""

        try:
            with self._lock:
                row = self._con.execute(
                    "SELECT status, created_at, updated_at FROM tasks WHERE id=?",
                    (task_id,),
                ).fetchone()
            if row is None:
                return None
            return {"id": task_id, "status": row[0], "created_at": row[1],
                    "updated_at": row[2]}
        except Exception as e:
            log.warning("journal get_meta(%s) failed: %s", task_id, e)
            return None

    def incomplete_tasks(self) -> list[str]:
        """Resumable task ids — non-terminal AND not paused. A paused task
        stays parked until an explicit resume signal attaches a runner, so
        ticks don't accumulate one parked asyncio.Task per paused row."""
        try:
            with self._lock:
                rows = self._con.execute(
                    f"SELECT id FROM tasks WHERE status IN "
                    f"({','.join('?' * len(_RESUMABLE_VALUES))})",
                    _RESUMABLE_VALUES,
                ).fetchall()
            return [r[0] for r in rows]
        except Exception as e:
            log.warning("journal incomplete_tasks failed: %s", e)
            return []

    # --- steps (the memoization record) -------------------------------------

    def step_result(self, task_id: str, step_key: str) -> dict[str, Any] | None:
        """Memoized result for a done step, else None (missing/failed/torn)."""
        try:
            with self._lock:
                row = self._con.execute(
                    "SELECT status,result_json FROM steps WHERE task_id=? AND step_key=?",
                    (task_id, step_key),
                ).fetchone()
            if row is None or row[0] != "done":
                return None
            return json_or(row[1], None)
        except Exception as e:
            log.warning("journal step_result(%s,%s) failed: %s", task_id, step_key, e)
            return None

    def step_begin(self, task_id: str, step_key: str, name: str) -> bool:
        try:
            with self._lock, self._con:
                self._con.execute(
                    "INSERT OR REPLACE INTO steps(task_id,step_key,name,status,"
                    "updated_at) VALUES (?,?,?,?,?)",
                    (task_id, step_key, name, "running", time.time()),
                )
            return True
        except Exception as e:
            log.warning("journal step_begin(%s,%s) failed: %s", task_id, step_key, e)
            return False

    def step_done(
        self, task_id: str, step_key: str, name: str, result: dict[str, Any]
    ) -> bool:
        """Serialize outside the transaction — a result that can't JSON must
        fail the step, not leave a torn 'running' row that re-executes."""
        try:
            payload = json.dumps(result)
        except Exception:
            self.step_failed(task_id, step_key, name, "step result not JSON-serializable")
            return False
        try:
            with self._lock, self._con:
                # upsert — survives a lost step_begin write
                self._con.execute(
                    "INSERT OR REPLACE INTO steps(task_id,step_key,name,status,"
                    "result_json,error,updated_at) VALUES (?,?,?,?,?,NULL,?)",
                    (task_id, step_key, name, "done", payload, time.time()),
                )
            return True
        except Exception as e:
            log.warning("journal step_done(%s,%s) failed: %s", task_id, step_key, e)
            return False

    def step_failed(self, task_id: str, step_key: str, name: str, error: str) -> bool:
        try:
            with self._lock, self._con:
                # upsert like step_done — a swallowed begin must not lose the failure
                self._con.execute(
                    "INSERT OR REPLACE INTO steps(task_id,step_key,name,status,"
                    "error,updated_at) VALUES (?,?,?,?,?,?)",
                    (task_id, step_key, name, "failed", str(error)[:2000], time.time()),
                )
            return True
        except Exception as e:
            log.warning("journal step_failed(%s,%s) failed: %s", task_id, step_key, e)
            return False
