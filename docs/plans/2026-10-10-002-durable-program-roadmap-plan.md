---
title: a0-plugin-durable program roadmap
type: program-roadmap
status: proposed
date: 2026-10-10
origin: docs/research/2026-10-10-landscape.md
---

# a0-plugin-durable program roadmap (2026-10-10)

Docs-only plan. Written by hand in the shape of `2026-10-08-001-feat-durable-plugin-plan.md`: the `ce-plan` skill was invoked with `mode:pipeline confirm:auto` but its multi-reference workflow was not run to completion, so this file has not been through its document-review step.

## Summary

Now: make the plugin do something in stock Agent Zero (U1) and check the claims it rests on (U2). Next: bound the journal (U3), add visibility (U4), decide the Restate engine's fate (U5). Later: bounded retries (U6), shared plugin scaffolding (U7).

| Horizon | Unit | Difficulty |
|---|---|---|
| Now | U1 Default `llm_call`/`tool_call` wiring into a0's agent loop | hard |
| Now | U2 Verify host contract and README claims against official docs | easy |
| Next | U3 Journal retention / prune | easy |
| Next | U4 Task list endpoint + minimal settings view | medium |
| Next | U5 Decide the Restate engine: keep with live test, or remove | medium |
| Later | U6 Opt-in retry policy for idempotent steps | medium |
| Later | U7 Shared config/runtime scaffolding across the five a0 plugins | medium |

## Current state (evidence)

- Local engine: `helpers/engines/local.py` (396 lines) plus `helpers/journal.py` (309), tests in `tests/test_local_engine.py`, `tests/test_journal.py`.
- Restate engine: `helpers/engines/restate.py` (514 lines), tests in `tests/test_restate_engine.py` are import-guarded; no live server is exercised in CI (`.github/workflows/ci.yml` installs only pytest).
- `llm_call` and `tool_call` have no default; only `api_call` is built in (`helpers/registry.py`). Without a host registration a submitted task fails cleanly (README, "Limits").
- No retention of `steps`/`tasks` rows; no list endpoint; no UI. `plugin.yaml` is version 0.1.0.
- README states Restate is "a single Rust binary"; the landscape research could not confirm that.

## Units

### U1. Default `llm_call` / `tool_call` wiring into a0's agent loop (Now)

- Status: not started. Evidence: `helpers/registry.py` registers only `api_call` by default; the plugin runs nothing on its own.
- Goal: an opt-in step registration that drives a0's real model and tool calls, so a task submitted via `durable_submit` does useful work.
- Dependencies: U2 (confirm what a0 exposes to plugins).
- Verification: an offline test with a stub a0 runtime that registers the steps, kills the engine mid-task and resumes with call counts unchanged; manual run on a real a0.
- **Difficulty**: hard. Needs a0 internals that the plugin has never touched; the existing tests stub `helpers.*`.
- **Feasibility**: blocked if a0 exposes no supported way to call its model and tools from a plugin; a0 internals are unverified here. Tool side effects stay at-least-once. No cost beyond model calls during manual test.
- **Simpler alternative**: document the `register_step` recipe in the README and ship it as an example file under `docs/` instead of shipping code; the host already has the hook.

### U2. Verify host contract and README claims (Now)

- Status: not started. Evidence: landscape section 1 of the shared research rests on third-party mirrors; the README "single Rust binary" and Temporal cluster claims were not confirmed.
- Goal: read the official `agent0ai/agent-zero` plugin docs and Restate docs, correct `README.md`, `AGENTS.md`, `plugin.yaml` where wrong.
- Dependencies: none.
- Verification: each corrected claim carries a source link in the PR; `python3 -m pytest tests/ -q` still passes.
- **Difficulty**: easy. Reading and editing docs.
- **Feasibility**: official docs may be thin or change; Restate's licence and packaging need a primary-source check.
- **Simpler alternative**: delete the unverified claims instead of proving them (the Temporal comparison paragraph in `README.md`).

### U3. Journal retention (Next)

- Status: not started. Evidence: README "Limits": rows accumulate for the life of `data/durable.sqlite3`.
- Goal: opt-in `retention_days` that deletes terminal tasks and their steps on the `job_loop` tick.
- Dependencies: none.
- Verification: new tests in `tests/test_journal.py`: old terminal rows removed, running and paused rows untouched.
- **Difficulty**: easy. One DELETE pair in `helpers/journal.py` and a config key (touches `DEFAULTS`, `default_config.yaml`, `_ENV_MAP`, README together).
- **Feasibility**: none external. Care needed so a deleted id can be re-submitted (submit is idempotent by id).
- **Simpler alternative**: document "delete the file when idle" and skip the code.

### U4. Task list endpoint and minimal settings view (Next)

- Status: not started. Evidence: only `durable_submit`, `durable_status`, `durable_signal` exist; no way to see tasks.
- Goal: `durable_list` returning bounded metadata, and a read-only list in the settings panel using the Patchbay status chips in `DESIGN.md`.
- Dependencies: U1 gives it something to show.
- Verification: API test in `tests/test_extensions_api.py`; screenshot from a running a0 (left as TODO in the README now).
- **Difficulty**: medium. The API is small; the panel depends on a0's webui extension points, which are unverified.
- **Feasibility**: a0 webui plugin surface is only known from third-party mirrors.
- **Simpler alternative**: ship `durable_list` only and read it with curl; skip the panel.

### U5. Decide the Restate engine (Next)

- Status: not started. Evidence: `helpers/engines/restate.py` is the largest module (514 lines) and has never run against a live server in CI.
- Goal: either add a live integration job (Restate server in CI plus `restate_sdk`, `hypercorn`) or remove the engine.
- Dependencies: U2 for the Restate facts.
- Verification: keep path: a CI job that submits, pauses, resumes and cancels on a real server. Remove path: tests pass without `tests/test_restate_engine.py`, README engine table reduced.
- **Difficulty**: medium. The live test needs a server in CI; removal is mostly deletion but touches `helpers/runtime.py` engine selection and `helpers/config.py` keys.
- **Feasibility**: Restate server packaging and licence in CI unverified; SDK versions may drift.
- **Simpler alternative**: delete `helpers/engines/restate.py`, `tests/test_restate_engine.py`, the `restate_*`/`listen_port` keys and the Restate README sections. That removes roughly a quarter of the helper code and the only optional dependency.

### U6. Opt-in retry policy for idempotent steps (Later)

- Status: not started. Evidence: README "Limits": fail-fast, no in-engine retry.
- Goal: per-step `retries` for steps flagged idempotent, with backoff, journaled attempts.
- Dependencies: U1.
- Verification: tests in `tests/test_local_engine.py`: a flaky step succeeds on attempt 2 and is memoized once.
- **Difficulty**: medium. Touches the journal row lifecycle (torn `running` rows reset on open).
- **Feasibility**: only safe for steps whose idempotency the host guarantees; the plugin cannot verify that.
- **Simpler alternative**: let hosts retry inside their own registered step function; no plugin change.

### U7. Shared scaffolding across the five plugins (Later)

- Status: not started. Evidence: shared landscape section 6; `helpers/config.py` and `helpers/runtime.py` patterns repeat across the sibling repos.
- Goal: agree one vendored `config.py` (DEFAULTS < plugin config < env, `truthy`, `num`) and one CI template.
- Dependencies: sibling repos' agreement.
- Verification: identical file hashes across repos, enforced by a small check.
- **Difficulty**: medium. Mostly coordination.
- **Feasibility**: a0 plugins are standalone directories, so sharing means copying or a sixth plugin.
- **Simpler alternative**: do nothing; `helpers/config.py` is 74 lines.

## Merge / retire assessment

Verdict: keep standalone; do not merge into glitchtip, notion, device-sync or jev-compact, and do not retire it yet.

Reasoning: durable execution is a different concern from error tracking, Notion access, device sync or context compaction, and merging would couple unrelated optional dependencies and settings. The research shows credible alternatives (Restate, DBOS, Temporal, LangGraph checkpointers), but none is wired into Agent Zero's loop, and the shared research found no a0 mechanism that resumes an interrupted agent loop (low confidence). The honest weakness is that without U1 the plugin does nothing in stock a0. If U1 proves infeasible, retire or archive it. The best simplification inside the plugin is U5's removal path: drop the Restate engine unless a user needs it, keeping the zero-dependency local engine.

## Verification of this plan

Docs only. `python3 -m pytest tests/ -q` must stay green; no application code changed.
