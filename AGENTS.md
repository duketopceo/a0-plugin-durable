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
                      CheckpointData, AgentStateSnapshot, ToolIdempotencyKey,
                      plus shared loop helpers (normalize_task_input,
                      iter_cap, loop_inputs, tool_calls_of, tool_message)
helpers/journal.py    SQLite store — tasks + steps tables (WAL, FULL sync)
helpers/registry.py   step-name -> async callable map (host wiring)
helpers/config.py     DEFAULTS < get_plugin_config < env; truthy()/num()
helpers/runtime.py    engine selection, never-raise facade, tick()
helpers/engines/base.py      Engine Protocol — the contract both engines share
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
- **Checkpoint boundary kills replay duplication.** `state.context_
  snapshot["checkpointed_through"]` records the last checkpointed
  iteration; replayed iterations `<= boundary` consume memos WITHOUT
  re-appending tool messages (they're already inside checkpointed
  `prompt_messages`). Read it with `is not None`, never `or -1` — a real
  boundary of 0 is falsy.
- **Pause survives restart — and parks.** A `paused` row is excluded from
  `incomplete_tasks()` so ticks don't accumulate parked runners; `signal
  ("resume")` attaches on demand and restores `executing` without
  overwriting a concurrently-landed `cancel` (`non_terminal_only` CAS).
  Local pause is a bare `asyncio.Event` wait (single-process journal can
  never miss a wake); Restate pause resolves durable promises
  `pause_{cycle}`/`resume_{cycle}` — the cycle counter is owned by the run
  handler because shared handlers are READ-ONLY for K/V state (`ctx.set`
  is illegal there — a fixed promise name would wedge after one pause).
- **Deferred-cap drain.** Over `max_concurrent`, `attach` defers (the row
  stays resumable — the journal IS the backlog). A finishing runner's
  `finally` re-scans `incomplete_tasks` so deferred work drains without
  waiting for the next tick; `_stopping` blocks attaches during teardown.
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
- **Writes return bools; terminal writes win.** `update_task`/`step_done`/
  `step_failed` return False on failure — engines treat a False as 'don't
  trust it'. `non_terminal_only` makes a status write a CAS so a landed
  signal/terminal state can't be clobbered by a stale runner write — a
  losing CAS returns False (rowcount-checked), and `signal()` uses it to
  close the read-then-write race against a concurrent terminal.
  Serialization happens BEFORE the transaction, so a poisoned result
  fails the step instead of rolling back a committed terminal status.
- **A result that can't JSON is a failed step.** `_step` validates
  serialization before journaling — an unjournaled result is never
  trusted (it would re-execute the side effect on replay).
- **Wire state is never trusted.** `normalize_task_input` drops a
  submitted `state` — engines hydrate from their own journal. Task ids
  are regex-validated (they land in PRIMARY KEYs and URL paths).
- **Registry lifecycle.** `register_defaults` uses setdefault — a host
  may register its own `api_call` before OR after `configure()`.
  Registrations survive `runtime.shutdown()`; `runtime._reset()`/
  `hooks.uninstall()` clear them (engine stop first — clearing the
  registry under live runners turns their next step into a bogus
  'no step registered' failure).
- **configure() keeps a live engine.** It refuses to swap engines while
  one is armed (orphaned runners would double-execute); `shutdown()`
  first to reconfigure. `tick()` re-reads `enabled` per pass ONLY when
  the config came from the host (`get_config`), not a programmatic dict.
- **Never-raise rule.** Everything public (`configure`, `tick`, `submit`,
  `status`, `signal`, extensions, `hooks`) is failure-contained — a durable
  misconfig or dead Restate must never take a0 down.

## When touching...

- Journal schema → bump nothing (plugin is pre-1.0) but migrate `init`
  reset logic if `steps` columns change.
- Engine interface → `helpers/engines/base.py` is THE contract
  (`start`/`submit`/`signal`/`status`/`meta`/`resume_incomplete`/`stop`).
  `attach`/`_runners`/`_wake` are local internals — a new engine must
  implement the Protocol, not local's internals. `runtime.tick()` calls
  `resume_incomplete()` on both (restate no-ops — the server replays
  independently).
- Shared loop helpers → `contract.py` owns `normalize_task_input`,
  `iter_cap`, `loop_inputs`, `tool_calls_of`, `tool_message` — engines
  MUST consume these instead of re-shaping inline (that's how the two
  engines stay behavior-identical).
- Restate ingress client → `_post` goes through `_SameOriginRedirect`:
  a 307/308 would re-POST the task body to whatever Location says, so
  only same-origin hops are followed; cross-origin raises HTTPError.
- Restate signals → shared handlers resolve durable promises ONLY —
  `ctx.set` from a shared handler is illegal in the SDK. Pause names are
  `pause_{cycle}`/`resume_{cycle}` versioned by the run handler's
  `pause_cycle` K/V. Peek-before-resolve keeps repeats idempotent.
- Config keys → `DEFAULTS`, `default_config.yaml`, `_ENV_MAP`, README
  all move together.
- Extension prefixes → `_60_` keeps durable after housekeeping jobs; sync
  for startup, async for job_loop. Plugin imports stay INSIDE `execute()`
  — a module-scope import failure would abort a0's whole extension sweep.
