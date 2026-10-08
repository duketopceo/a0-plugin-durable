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
"""


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
        self._con.execute("PRAGMA synchronous=NORMAL")
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
            return {
                "status": row[0],
                "input": json_or(row[1], {}),
                "state": json_or(row[2], {}),
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
    ) -> None:
        try:
            with self._lock, self._con:
                if status is not None:
                    self._con.execute(
                        "UPDATE tasks SET status=?, updated_at=? WHERE id=?",
                        (status.value, time.time(), task_id),
                    )
                if state is not None:
                    self._con.execute(
                        "UPDATE tasks SET state_json=?, updated_at=? WHERE id=?",
                        (state.to_json(), time.time(), task_id),
                    )
        except Exception as e:
            log.warning("journal update_task(%s) failed: %s", task_id, e)

    def incomplete_tasks(self) -> list[str]:
        """Task ids the runner should resume after a restart."""
        try:
            with self._lock:
                rows = self._con.execute(
                    "SELECT id FROM tasks WHERE status NOT IN (?,?)",
                    tuple(TERMINAL_VALUES),
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

    def step_begin(self, task_id: str, step_key: str, name: str) -> None:
        try:
            with self._lock, self._con:
                self._con.execute(
                    "INSERT OR REPLACE INTO steps(task_id,step_key,name,status,"
                    "updated_at) VALUES (?,?,?,?,?)",
                    (task_id, step_key, name, "running", time.time()),
                )
        except Exception as e:
            log.warning("journal step_begin(%s,%s) failed: %s", task_id, step_key, e)

    def step_done(
        self, task_id: str, step_key: str, name: str, result: dict[str, Any]
    ) -> None:
        try:
            with self._lock, self._con:
                # upsert — survives a lost step_begin write
                self._con.execute(
                    "INSERT OR REPLACE INTO steps(task_id,step_key,name,status,"
                    "result_json,error,updated_at) VALUES (?,?,?,?,?,NULL,?)",
                    (task_id, step_key, name, "done", json.dumps(result), time.time()),
                )
        except Exception as e:
            log.warning("journal step_done(%s,%s) failed: %s", task_id, step_key, e)

    def step_failed(self, task_id: str, step_key: str, name: str, error: str) -> None:
        try:
            with self._lock, self._con:
                # upsert like step_done — a swallowed begin must not lose the failure
                self._con.execute(
                    "INSERT OR REPLACE INTO steps(task_id,step_key,name,status,"
                    "error,updated_at) VALUES (?,?,?,?,?,?)",
                    (task_id, step_key, name, "failed", str(error)[:2000], time.time()),
                )
        except Exception as e:
            log.warning("journal step_failed(%s,%s) failed: %s", task_id, step_key, e)
