# Plan: a0-plugin-durable — durable agent execution (Khan#194)

## Source / intent

Khan issue [#194](https://github.com/duketopceo/Khan/issues/194) — extract
`~/Khan/helpers/durable/` (~1260 LOC, Temporal-shaped, never wired into
stock a0) into a standalone plugin. Direction per the issue: engine-agnostic
contract, Restate first, Hatchet later; **no Temporal cluster dependency**.

## Settled decisions (from issue + recon)

- `contract.py` ports **verbatim** — `TaskStatus`, `TaskState`,
  `CheckpointData`, `AgentStateSnapshot`, `ToolIdempotencyKey` are already
  JSON-safe and engine-agnostic. Only the `to_temporal()` seam is dropped
  (RetryPolicy stays plain ms-ints; the Restate adapter converts to
  `restate.RunOptions`).
- **Two engines behind one interface.** `local` (default): SQLite journal in
  the plugin's usr dir — zero deps, restart-survivable, IS the
  "APScheduler+SQLite is enough" path the issue asks to document. `restate`:
  guarded `restate_sdk` import + Restate server endpoint — real durable
  engine for operators who run the sidecar binary.
- **In-process endpoint, not subprocess.** Restate invokes handlers over
  HTTP; the plugin serves `restate.app` on a dedicated thread inside a0 so
  handlers reach the host step registry. A subprocess worker can't call
  in-process a0 functions (Khan's `_call`-in-args injection is unshippable —
  callables don't serialize across a real engine wire).
- **Step registry replaces `_call`/`_result`/`_transport` injection.**
  `llm_call` / `tool_call` / `api_call` are named steps the host registers
  callables against at configure time; engines execute by name, never by
  serialized function.
- `job_loop` extension = periodic reconcile tick (idempotent re-submit of
  `tasks[]` from config; resume incomplete local tasks). NOT the execution
  driver — engines drive execution.
- Never-raise contract identical to a0-plugin-glitchtip (same a0 loading
  mechanics): extension `execute` bodies fully guarded; startup init is
  sync under `call_extensions_sync`.

## Architecture

```
plugin.yaml              manifest, settings_sections: external
default_config.yaml      enabled, engine(local|restate), restate_ingress,
                         restate_listen(port 0=auto), journal_path, tasks[],
                         step_timeout_s, max_iterations
hooks.py                 install(): log; uninstall(): stop engine + endpoint
helpers/
  contract.py            TaskState/CheckpointData/ToolIdempotencyKey (verbatim port)
  config.py              DurableConfig (plugin-cfg + env), RetryPolicy (ms ints)
  registry.py            step name -> callable; host registers llm/tool/api steps
  journal.py             SQLite step journal: begin/complete/fail + task rows
  engine.py              Engine protocol: submit/signal/query/cancel/status
  engines/local.py       asyncio runner + journal replay (resume on boot)
  engines/restate.py     restate.Workflow + ctx.run steps + ASGI endpoint thread
  runtime.py             facade: configure/submit/pause/resume/cancel/status/reset
extensions/python/
  startup_migration/_25_durable_init.py   configure engine; resume local tasks
  job_loop/_30_durable_tick.py            reconcile config tasks[] idempotently
api/
  durable_submit.py      POST start a durable task
  durable_status.py      POST query task state
  durable_signal.py      POST pause/resume/cancel
tests/                   conftest (a0 stubs, tmp journal), contract/engine/
                         local/replay/registry/api coverage — offline
```

## Acceptance → test mapping

| Issue criterion | Test |
|---|---|
| `job_loop` schedules durable tasks on stock a0 | tick submits config task, second tick is idempotent no-op |
| workflow survives restart | local engine: run task, drop at step N, new engine on same journal, resume — journaled steps NOT re-executed (memoization proven by call counts) |
| `uninstall()` stops services | hooks.uninstall stops runner + ASGI thread; port freed |
| docs: engine choice + APScheduler note | README + AGENTS.md |

## Degradation contract

- `enabled: false` or no engine configured → everything inert.
- `engine: restate` without `restate_sdk`/`hypercorn` installed → plugin
  logs a clear "pip install restate_sdk hypercorn" warning, stays inactive;
  local engine unaffected.
- Journal corruption → that task marked FAILED, other tasks unaffected.
- Every public helper + extension never raises into host.

## Explicitly out of scope

- Temporal adapter port (reference only — documented in README).
- Hatchet engine (contract is engine-agnostic; follow-up issue).
- Scheduling/cron syntax — `tasks[]` is a flat declarative list; richer
  scheduling is a later plugin (`a0-plugin-scheduler` territory).
- WebUI surfaces beyond `default_config.yaml` settings.

## Test strategy

Offline. `tests/conftest.py` synthesizes `usr.plugins.durable` namespace +
`helpers.extension`/`helpers.plugins`/`helpers.api` stubs (same pattern as
a0-plugin-glitchtip). Restate tests are import-guarded and assert graceful
degradation when the SDK is absent — no restate server in CI.
