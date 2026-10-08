"""Durable execution task-state contract — engine-agnostic.

Serializable task state, checkpoint, and agent-snapshot structures shared
between the engine adapters and the a0 agent loop. These types carry only
JSON-serializable primitives so they cross engine boundaries (SQLite journal
rows, Restate journal entries) without referencing live agent objects.

Ported from Khan `helpers/durable/contract.py` — the Temporal ``RetryPolicy``
seam was dropped: local steps are bounded by ``step_timeout_s`` and Restate
retry policy is server-configured, so no per-step retry options cross the
engine boundary.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class TaskStatus(StrEnum):
    """Lifecycle status for a durable agent task.

    Inherited from a string enum so values serialize naturally to JSON and are
    comparable by identity or value. ``CHECKPOINTED`` is reserved — persisted
    for contract parity but not yet written by either shipped engine.
    """

    CREATED = "created"
    PLANNED = "planned"
    EXECUTING = "executing"
    CHECKPOINTED = "checkpointed"
    PAUSED = "paused"
    RESUMED = "resumed"
    COMPLETED = "completed"
    FAILED = "failed"

    def can_transition_to(self, target: TaskStatus) -> bool:
        """Best-effort transition guard for the documented lifecycle.

        The lifecycle is permissive by design — the engine drives the real
        transitions — but this helper lets callers assert obvious illegal
        jumps (e.g. COMPLETED -> EXECUTING).
        """

        if self is target:
            return True
        return self not in TERMINAL_STATUSES


# Single source of truth for "no further transitions" — the journal query,
# engine runners, and signal handling all consume this.
TERMINAL_STATUSES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.COMPLETED, TaskStatus.FAILED}
)
TERMINAL_VALUES: frozenset[str] = frozenset(s.value for s in TERMINAL_STATUSES)

# api/engine signal verbs -> the status they persist. Membership in this map
# is the validity check everywhere.
SIGNAL_ACTIONS: dict[str, TaskStatus] = {
    "pause": TaskStatus.PAUSED,
    "resume": TaskStatus.RESUMED,
    "cancel": TaskStatus.FAILED,  # contract has no 'cancelled' — cancel is terminal
}


@dataclass
class TaskState:
    """Serializable snapshot of a single durable task's progress.

    ``tool_results`` maps idempotency keys to tool result dicts so replayed
    steps stay deterministic. ``model_state`` carries provider-side state
    (response ids, capability metadata) that must survive a checkpoint.
    """

    id: str
    status: TaskStatus = TaskStatus.CREATED
    context_snapshot: dict[str, Any] = field(default_factory=dict)
    plan: list[dict[str, Any]] = field(default_factory=list)
    tool_results: dict[str, Any] = field(default_factory=dict)
    artifacts: list[Any] = field(default_factory=list)
    model_state: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def touch(self, now: datetime | None = None) -> None:
        """Refresh ``updated_at``. Engines may pass their deterministic
        clock; callers that omit ``now`` get the wall clock."""

        self.updated_at = now if now is not None else datetime.now(UTC)

    def transition_to(self, target: TaskStatus, now: datetime | None = None) -> None:
        """Move the task to ``target`` status, updating ``updated_at``.

        Raises ``ValueError`` for obviously illegal transitions out of terminal
        states so callers fail fast instead of silently corrupting state.
        """

        if not self.status.can_transition_to(target):
            raise ValueError(f"Illegal task status transition: {self.status.value} -> {target.value}")
        self.status = target
        self.touch(now=now)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-safe dict with deep-copied mutable fields.

        Mutable containers are deep-copied so a serialized snapshot cannot be
        mutated by later changes to the live instance.
        """

        return {
            "id": self.id,
            "status": self.status.value,
            "context_snapshot": copy.deepcopy(self.context_snapshot),
            "plan": copy.deepcopy(self.plan),
            "tool_results": copy.deepcopy(self.tool_results),
            "artifacts": copy.deepcopy(self.artifacts),
            "model_state": copy.deepcopy(self.model_state),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any], now: datetime | None = None) -> TaskState:
        """Reconstruct a ``TaskState`` from a serialized dict.

        ``now`` supplies the fallback timestamp for missing ``created_at`` /
        ``updated_at`` fields so engines with deterministic clocks can supply
        them; wall clock otherwise.
        """

        return cls(
            id=data["id"],
            status=TaskStatus(data.get("status", TaskStatus.CREATED.value)),
            context_snapshot=dict(data.get("context_snapshot", {})),
            plan=list(data.get("plan", [])),
            tool_results=dict(data.get("tool_results", {})),
            artifacts=list(data.get("artifacts", [])),
            model_state=dict(data.get("model_state", {})),
            created_at=_parse_dt(data.get("created_at"), now=now),
            updated_at=_parse_dt(data.get("updated_at"), now=now),
        )

    def _wire(self) -> dict[str, Any]:
        """Serialization shape without the defensive deepcopy — json.dumps
        never mutates, so the per-checkpoint copy is pure overhead. External
        callers use ``to_dict()``; the journal hot path uses this."""

        return {
            "id": self.id,
            "status": self.status.value,
            "context_snapshot": self.context_snapshot,
            "plan": self.plan,
            "tool_results": self.tool_results,
            "artifacts": self.artifacts,
            "model_state": self.model_state,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    def to_json(self) -> str:
        """Serialize to a JSON string.

        Non-JSON values raise instead of being silently stringified, so type
        drift across checkpoints is caught immediately.
        """

        return json.dumps(self._wire(), sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> TaskState:
        """Reconstruct from a JSON string."""

        return cls.from_dict(json.loads(raw))


@dataclass
class CheckpointData:
    """A single iteration checkpoint captured between step calls.

    Reserved — ported for contract parity with Khan but not yet consumed by
    either shipped engine (local checkpoints via ``state_json``, restate via
    ``ctx.set``). Do not wire assumptions onto this type.
    """

    iteration: int
    prompt_messages: list[dict[str, Any]] = field(default_factory=list)
    llm_result: dict[str, Any] = field(default_factory=dict)
    tool_calls: list[dict[str, Any]] = field(default_factory=list)
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict[str, Any]:
        return {
            "iteration": self.iteration,
            "prompt_messages": self.prompt_messages,
            "llm_result": self.llm_result,
            "tool_calls": self.tool_calls,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any], now: datetime | None = None) -> CheckpointData:
        return cls(
            iteration=int(data.get("iteration", 0)),
            prompt_messages=list(data.get("prompt_messages", [])),
            llm_result=dict(data.get("llm_result", {})),
            tool_calls=list(data.get("tool_calls", [])),
            timestamp=_parse_dt(data.get("timestamp"), now=now),
        )


@dataclass
class AgentStateSnapshot:
    """Serializable agent state for provider handoff / checkpoint restore.

    Reserved — ported for contract parity; neither shipped engine consumes it
    yet (the runner restores from ``TaskState``, not a live ``Agent``).
    """

    context: dict[str, Any] = field(default_factory=dict)
    history: list[dict[str, Any]] = field(default_factory=list)
    loop_data: dict[str, Any] = field(default_factory=dict)
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "context": self.context,
            "history": self.history,
            "loop_data": self.loop_data,
            "data": self.data,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AgentStateSnapshot:
        return cls(
            context=dict(data.get("context", {})),
            history=list(data.get("history", [])),
            loop_data=dict(data.get("loop_data", {})),
            data=dict(data.get("data", {})),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> AgentStateSnapshot:
        return cls.from_dict(json.loads(raw))


# Known host-injection seams excluded from the idempotency hash. Only these
# exact names are filtered — a model-supplied ``_anything_else`` still feeds
# the digest, so different args can never share a memoized result.
_INJECTION_ARGS = frozenset({"_call", "_result", "_transport"})


class ToolIdempotencyKey:
    """Deterministic idempotency key for tool execution.

    Builds a stable key from the tool name plus a SHA-256 hash of the sorted
    arguments. The same ``(tool_name, tool_args)`` pair always yields the same
    key, so a replayed tool step can short-circuit on a cached result stored
    under that key in ``TaskState.tool_results``.

    Only the named host-injection seams (``_call``/``_result``/``_transport``)
    are excluded before hashing, and non-JSON-serializable argument values
    raise ``TypeError`` instead of producing a meaningless digest.
    """

    @staticmethod
    def build(tool_name: str, tool_args: dict[str, Any] | None) -> str:
        normalized = tool_name or ""
        args = tool_args or {}
        # Exclude the named injection seams from the identity.
        filtered = {k: v for k, v in args.items() if k not in _INJECTION_ARGS}
        # Sorted, separators-stable JSON so dict ordering never changes the hash.
        # No default=str: non-JSON args raise TypeError rather than hashing a
        # meaningless stringification.
        payload = json.dumps(filtered, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return f"{normalized}:{digest}"


def _parse_dt(value: Any, now: datetime | None = None) -> datetime:
    """Parse an ISO datetime string, making naive values UTC-aware.

    Aware ``datetime`` values pass through unchanged; naive ones are assumed
    UTC — same convention as naive ISO strings. When ``value`` is absent,
    ``now`` is used if supplied (an engine's deterministic clock); otherwise
    the wall clock.
    """

    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            pass
        else:
            return _parse_dt(parsed, now=now)
    return now if now is not None else datetime.now(UTC)


# --- shared agent-loop data-shaping -----------------------------------------
# Both engines run the same llm_call → tool_calls → checkpoint loop; these
# helpers keep the pure parts identical so the engines cannot drift.


_TASK_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
# Submitted task_input cap — the whole dict is journaled per task.
MAX_INPUT_BYTES = 1 << 20  # 1 MiB


def normalize_task_input(task_input: dict[str, Any] | None) -> tuple[str, dict[str, Any]]:
    """Resolve the task id — explicit ``id``, then ``state.id``, else a fresh
    uuid4 — and return ``(task_id, input)`` with ``id`` promoted to top level.
    Id-keyed dedupe is what makes re-submission safe on every engine.

    Raises ``ValueError`` on an invalid caller-supplied id (it lands in sqlite
    PRIMARY KEYs, Restate URL paths, and log lines) and on oversized input.
    A caller-supplied ``state`` is dropped — engines hydrate task state from
    their own journal, never from the wire, so a submission cannot forge
    memoized step results or prompt history.
    """

    data = dict(task_input or {})
    raw_id = data.get("id") or (data.get("state") or {}).get("id")
    task_id = str(raw_id) if raw_id is not None else uuid.uuid4().hex
    if not _TASK_ID_RE.fullmatch(task_id):
        raise ValueError(f"invalid task id: {task_id!r}")
    data.pop("state", None)  # see docstring — inbound state is never trusted
    data["id"] = task_id
    if len(json.dumps(data)) > MAX_INPUT_BYTES:
        raise ValueError("task input exceeds 1 MiB")
    return task_id, data


def iter_cap(task_input: dict[str, Any], default: int) -> int:
    """Per-task ``max_iterations`` override with the engine default as
    fallback. Missing/invalid -> ``default``; valid values clamp to
    ``[0, default * 10]`` so a submitter can't request an unbounded loop."""

    raw = task_input.get("max_iterations")
    if raw is None:
        return default
    try:
        val = int(raw)
    except (TypeError, ValueError):
        return default
    return min(max(val, 0), max(default, 1) * 10)


def loop_inputs(state: TaskState, task_input: dict[str, Any]) -> tuple[list, dict]:
    """(messages, model_config) — checkpointed values win over the raw input
    so a resumed task continues from where it checkpointed."""

    messages = list(
        state.context_snapshot.get("prompt_messages") or task_input.get("prompt_messages") or []
    )
    model_cfg = dict(
        state.model_state.get("model_config") or task_input.get("model_config") or {}
    )
    return messages, model_cfg


def tool_calls_of(llm_result: dict[str, Any] | None) -> list[dict[str, Any]]:
    if isinstance(llm_result, dict):
        calls = llm_result.get("tool_calls")
        if isinstance(calls, list):
            return calls
    return []


def tool_message(name: str, tool_result: dict[str, Any]) -> dict[str, Any]:
    return {"role": "tool", "name": name, "content": tool_result.get("result", "")}


def json_or(raw: Any, default: Any = None) -> Any:
    """json.loads that tolerates bytes/str/None and never raises."""

    try:
        return json.loads(raw) if raw else default
    except Exception:
        return default
