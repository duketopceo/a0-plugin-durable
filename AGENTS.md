# a0-plugin-durable — agent notes

Durable execution plugin for Agent Zero. `README.md` is user-facing; this file
is what an agent needs that the README does not say.

## Commands

```bash
# Tests — standalone, offline, no a0 checkout required
python3.12 -m pytest tests/ -q
```

## Layout

```
helpers/contract.py   ported Khan contract: TaskStatus, TaskState,
                      CheckpointData, AgentStateSnapshot, ToolIdempotencyKey
helpers/journal.py    SQLite store — tasks + steps tables (WAL)
helpers/registry.py   step-name -> async callable map (host wiring)
helpers/config.py     DEFAULTS < get_plugin_config < env; truthy()/num()
helpers/runtime.py    engine selection, never-raise facade, tick()
helpers/engines/local.py     zero-dep journal runner (default)
helpers/engines/restate.py   guarded restate_sdk + hypercorn adapter
extensions/python/startup_migration/_60_durable_init.py   sync configure()
extensions/python/job_loop/_60_durable_reconcile.py       async tick()
api/durable_{submit,status,signal}.py                     ApiHandler trio
hooks.py              install()/uninstall() — stops owned services
tests/                standalone conftest stubs helpers.{extension,plugins,api}
```

## Non-obvious contracts

- **The engine boundary is JSON-only.** Khan's Temporal adapter injected live
  callables into activity args (`_call`, `_result`) — that can never cross a
  real engine's wire. Here, engines call `registry.get_step(name)` by string
  and the registered closure holds the live a0 runtime. Never pass a callable
  through `submit()`/`TaskState`.
- **Journal = source of truth for control; `state_json` = checkpoint
  payload.** `TaskState.status` mirrors the row, but the `tasks.status`
  column is what signals and `_halted`/`_await_if_paused` read.
- **Torn 'running' steps reset on open.** `Journal.__init__` flips
  `running → failed` — a resumed task re-executes the step rather than
  trusting a partial write. `step_result` only returns `done` rows.
- **Pause survives restart.** A task row left `paused` at shutdown stays
  paused when re-attached (`_run` checks the row before forcing EXECUTING).
  Local pause is a bare `asyncio.Event` wait (the journal is single-process,
  so `signal()` can never miss a wake); Restate pause awaits a durable
  promise `resume_{pause_epoch}` — zero journal churn either way.
- **Terminal statuses are `{completed, failed}`.** `cancel` → `failed`
  (contract has no cancelled). `contract.TERMINAL_VALUES`/`SIGNAL_ACTIONS`
  are the single source of truth — journal, engines, and api all share them.
- **`tasks.status` is authoritative.** `state_json.status` mirrors it for
  serialization but signals only write the column — `status()` overlays
  column state onto the state dict before returning.
- **`startup_migration` is SYNC.** a0's `call_extensions_sync` raises if
  `execute()` returns an awaitable — `_60_durable_init.py` must stay
  `def`, not `async def`. Restate serving starts lazily on first async
  touch for the same reason.
- **Idempotent submit by id.** `create_task` is `INSERT OR IGNORE`; a
  re-submitted id is a no-op — that's what makes `job_loop` ticks safe.
- **Same idempotency key ⇒ same memoized result** — but only inside one
  task (`step_key` embeds the iteration). Two different tasks calling the
  same tool both execute.
- **Never-raise rule.** Everything public (`configure`, `tick`, `submit`,
  `status`, `signal`, extensions, `hooks`) is failure-contained — a durable
  misconfig or dead Restate must never take a0 down.

## When touching...

- Journal schema → bump nothing (plugin is pre-1.0) but migrate `init`
  reset logic if `steps` columns change.
- Engine interface (`submit`/`signal`/`status`/`stop`/`attach`/
  `resume_incomplete`) → keep all signatures identical across
  `local`/`restate`; `runtime.tick()` calls `resume_incomplete()` on both
  (restate no-ops — the server replays independently).
- Config keys → `DEFAULTS`, `default_config.yaml`, `_ENV_MAP`, README table
  all move together.
- Extension prefixes → `_60_` keeps durable after housekeeping jobs; sync
  for startup, async for job_loop.
