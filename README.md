# a0-plugin-durable

Durable, restart-survivable agent tasks for [Agent Zero](https://github.com/agent0ai/agent-zero) — extracted from the Khan fork's Temporal-shaped `helpers/durable/`.

A "durable task" is an agent-loop-shaped unit of work — `llm_call` → `tool_call` × N → repeat — where **every step is journaled before its result is trusted**. If a0 restarts mid-task, journaled steps replay from their recorded results and execution resumes at the first unfinished step. Nothing re-runs that already ran.

Everything is opt-in: with `enabled: false` the plugin is fully inert and adds zero startup cost.

## Install

Drop into `usr/plugins/durable/` in an a0 checkout (see sibling plugins `a0-plugin-omaseal` / `a0-plugin-glitchtip` for the same layout). Enable it in the plugin settings or via env — `DURABLE_ENABLED`, `DURABLE_ENGINE`, `DURABLE_JOURNAL_PATH`, `DURABLE_RESTATE_INGRESS`, `DURABLE_RESTATE_ADMIN`, `DURABLE_LISTEN_PORT`, `DURABLE_MAX_CONCURRENT` map onto the keys in `default_config.yaml`.

## Engines

Two interchangeable engines behind one contract (`helpers/contract.py`):

| | `local` (default) | `restate` |
|---|---|---|
| Extra processes | none | Restate server sidecar (single Rust binary) |
| Python deps | none | `restate_sdk` + `hypercorn` |
| Survives a0 restart | yes — SQLite journal | yes — server-side journal + replay |
| Survives host reboot | resumes on next a0 start | server resumes independently of a0 |
| Scale-out / shared workers | no | yes |
| Console/UI, retries, timers | DIY | built-in |

### `local` — SQLite journal + in-process runner

Zero dependencies. Tasks are journaled into `data/durable.sqlite3` (WAL mode). The `job_loop` extension re-attaches runners for incomplete tasks on every tick, so a task interrupted by a process restart resumes automatically next tick. **Paused tasks stay parked** — they are excluded from auto-attach so ticks don't accumulate one idle runner per paused row; an explicit `resume` signal attaches a runner on demand. `max_concurrent` caps live runners; overflow stays resumable in the journal (the journal IS the backlog) and drains as runners finish.

Journal paths outside the plugin directory are allowed but warned about — WAL needs a local filesystem and exactly one writer process.

**This is the "APScheduler + SQLite is enough" answer.** If your tasks are periodic, must survive an a0 restart, and don't need cross-process failover or a console, the local engine is sufficient — do not stand up a Restate server for that. Reach for `restate` when you need: workers on different hosts than a0, durable timers measured in days, signal-driven human-in-the-loop at scale, or the Restate UI for inspecting journaled invocations.

### `restate` — Restate workflow over the same contract

```bash
pip install restate_sdk hypercorn
```

serve the endpoint in-process on `listen_port` (loopback only — Restate server runs beside a0 on the same host), register it with the Restate admin API, then submit via the ingress. Steps execute through the same host-side registry, journaled by Restate's execution log.

Signals ride **durable promises** — Restate shared handlers are read-only for K/V state, so `pause`/`resume`/`cancel` resolve promises the run handler observes at its pause gate. A `pause_cycle` counter keeps repeated pause/resume rounds un-wedgeable (durable promises resolve once, so names are versioned per cycle). Cancel also resolves the pending resume promise, un-sticking a parked gate — and always wins over a concurrent completion.

Registration/serving is retried lazily: if the admin API or endpoint bind fails, `start()` returns False and the next submit/status/tick tries again — a stuck `_started` flag can't wedge the engine.

The SDK imports are **lazy**: without them the plugin reports itself inactive instead of breaking a0 startup.

**Why not Temporal?** Khan's adapter targeted `temporalio`, which requires a Temporal cluster (Cassandra/Postgres + multiple services) — too heavy for an a0 plugin, and its activity API allowed injecting live callables into serialized arguments (a boundary violation that could never actually cross the wire). Restate is a single binary with an embedded store, and this plugin's registry resolves step callables by *name* on the serving side, so nothing non-serializable ever crosses an engine boundary. Temporal is retained in Khan only as the reference design.

## Task shape

`submit()` takes a JSON-safe dict:

```python
await runtime.submit({
    "id": "optional-stable-id",          # dedupe key; uuid4 if omitted
    "prompt_messages": [{"role": "user", "content": "..."}],
    "model_config": {},                  # provider/model params, host-defined
    "max_iterations": 25,                # optional per-task cap override
})
```

Input rules (enforced identically on both engines by `contract.normalize_task_input`):

- `id` must match `[A-Za-z0-9][A-Za-z0-9._-]{0,127}` — it lands in sqlite PRIMARY KEYs and Restate URL paths.
- Total input ≤ 1 MiB JSON.
- A submitted `state`/`checkpoint` is **dropped** — engines hydrate task state from their own journal; the wire can't forge memoized results.
- `max_iterations` ≤ `default * 10` — a submitter can tighten the loop, not unbound it.
- `max_iterations: 0` is legal — the task completes immediately with the cap note.

Steps are host-registered callables — `register_step(name, async fn)`:

| name | kwargs | returns |
|---|---|---|
| `llm_call` | `prompt_messages`, `model_config` | `{"tool_calls": [...]}` or terminal result |
| `tool_call` | `tool_name`, `tool_args`, `idempotency_key` | `{"result": ...}`, optional `"break_loop": true` |
| `api_call` | `method`, `url`, `headers`, `body` | `{"status", "headers", "body"}` — **built-in** |

`llm_call` and `tool_call` have no default — the host wires them to its own model/tool runtime. Deterministic `ToolIdempotencyKey`s dedupe identical tool calls inside a task.

The built-in `api_call` is a utility host steps can use for journaled HTTP: http/https only, **redirects refused** (urllib forwards `Authorization` cross-host on a 3xx), 30s timeout, response body capped at 1 MiB, sensitive response headers stripped before journaling. It is intentionally **SSRF-capable** — reaching internal services is a feature; the host decides which task inputs are trusted.

## API

| endpoint | body | returns |
|---|---|---|
| `durable_submit` | `{"input": {...}}` | `{ok, task_id}` |
| `durable_status` | `{"task_id": "..."}` | `{ok, state}` — bounded meta by default; `{"task_id": ..., "full": true}` for the serialized `TaskState` |
| `durable_signal` | `{"task_id": ..., "action": ...}` | `{ok}` — `pause`/`resume`/`cancel` |

All inherit a0 `ApiHandler` auth + CSRF, POST-only, and return `{ok: false, error}` on bad input (non-dict bodies included). `cancel` marks the task `failed` (the contract has no `cancelled` status); signals on unknown/terminal tasks return `ok: false`.

## Scheduled tasks

`tasks:` in config are submitted by the `job_loop` tick — idempotently by `id`, so editing a task's id is how you re-run one. The documented shape unwraps `input:` (same as the API):

```yaml
tasks:
  - id: nightly-cleanup
    enabled: true
    input:
      prompt_messages:
        - role: user
          content: "Clean up stale sessions."
```

Malformed/disabled entries are skipped without stalling the job loop. See `default_config.yaml` for the full settings list.

## Limits

- `max_iterations` (default 100) caps the agent loop per task; per-task overrides clamp to `default * 10`.
- `step_timeout_s` (default 300) bounds each journaled step on the local engine (Restate owns step timeouts/retry policy server-side).
- `max_concurrent` (default 8) caps live local runners — excess tasks wait in the journal and drain as runners finish.
- Semantics are **at-least-once** in the window between a step's side effect and its journal commit — side-effecting steps must be idempotent on `(tool_name, tool_args)` (that's what the idempotency key is for).
- Pause is event-driven on both engines — a paused local task costs nothing until signalled; a paused Restate workflow awaits a durable promise (no journal churn).
- Error model is fail-fast — a step exception fails the task. Deterministic failures do not retry (Restate: converted to `TerminalError` so server retries don't burn on a bug). Resilience comes from restart replay, not in-engine retry loops.
- The journal is append-only — `steps`/`tasks` rows accumulate for the life of `data/durable.sqlite3`. No retention policy yet; prune by deleting the file or older rows if it grows.
- Local engine is **single-process** — two a0 instances sharing one `journal_path` will double-execute in-flight steps (no cross-process lease).
- Restate endpoint binds loopback only — the server is a same-host sidecar; remote workers are not a shipped feature.
- `llm_call`/`tool_call` have no default wiring — the host registers them (see "Task shape"). Submitted tasks without an `llm_call` registration fail cleanly.
- Journal lives under `data/` — gitignored. Back it up or delete it with the plugin.

## Development

```bash
python3.12 -m pytest tests/ -q   # standalone, offline
```

`AGENTS.md` documents the internals an agent needs (journal schema, replay contract, registry).
