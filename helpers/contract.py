"""Durable execution task-state contract — engine-agnostic.

Serializable task state, checkpoint, and agent-snapshot structures shared
between the engine adapters and the a0 agent loop. These types carry only
JSON-serializable primitives so they cross engine boundaries (SQLite journal
rows, Restate journal entries) without referencing live agent objects.

Ported from Khan `helpers/durable/contract.py` — the Temporal-specific seam
(``RetryPolicy.to_temporal``) lives in ``engines/restate.py`` instead.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class TaskStatus(StrEnum):
    """Lifecycle status for a durable agent task.

    Inherited from a string enum so values serialize naturally to JSON and are
    comparable by identity or value.
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
        terminal = {TaskStatus.COMPLETED, TaskStatus.FAILED}
        return self not in terminal


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

    def to_json(self) -> str:
        """Serialize to a JSON string.

        Non-JSON values raise instead of being silently stringified, so type
        drift across checkpoints is caught immediately.
        """

        return json.dumps(self.to_dict(), sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> TaskState:
        """Reconstruct from a JSON string."""

        return cls.from_dict(json.loads(raw))


@dataclass
class CheckpointData:
    """A single iteration checkpoint captured between step calls.

    Persisted in the engine's journal/memo so a replayed task can resume from
    the exact prompt messages, LLM result, and pending tool calls without
    re-invoking the model.
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

    Captures the minimal, JSON-safe slice of an ``Agent`` instance required to
    resume the agent loop in a fresh process: the context data dict, the
    history output, a serialized ``LoopData``-equivalent dict, and the agent's
    free-form ``data`` dict. Live objects (model handles, log sinks) are not
    carried; they are reconstructed by the host on resume.
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


class ToolIdempotencyKey:
    """Deterministic idempotency key for tool execution.

    Builds a stable key from the tool name plus a SHA-256 hash of the sorted
    arguments. The same ``(tool_name, tool_args)`` pair always yields the same
    key, so a replayed tool step can short-circuit on a cached result stored
    under that key in ``TaskState.tool_results``.

    Underscore-prefixed arguments (host injection seams like ``_call``) are
    excluded before hashing, and non-JSON-serializable argument values raise
    ``TypeError`` instead of producing a meaningless digest.
    """

    @staticmethod
    def build(tool_name: str, tool_args: dict[str, Any] | None) -> str:
        normalized = tool_name or ""
        args = tool_args or {}
        # Exclude injection seams (underscore-prefixed keys) from the identity.
        filtered = {k: v for k, v in args.items() if not k.startswith("_")}
        # Sorted, separators-stable JSON so dict ordering never changes the hash.
        # No default=str: non-JSON args raise TypeError rather than hashing a
        # meaningless stringification.
        payload = json.dumps(filtered, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return f"{normalized}:{digest}"


def _parse_dt(value: Any, now: datetime | None = None) -> datetime:
    """Parse an ISO datetime string, making naive values UTC-aware.

    Already-aware ``datetime`` values pass through unchanged. Naive ISO strings
    are assumed to be UTC. When ``value`` is absent, ``now`` is used if supplied
    (an engine's deterministic clock); otherwise the wall clock.
    """

    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            pass
        else:
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed
    return now if now is not None else datetime.now(UTC)
