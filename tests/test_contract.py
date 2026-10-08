"""Contract port coverage: TaskState lifecycle/serialization, CheckpointData,
AgentStateSnapshot, ToolIdempotencyKey determinism."""

import pytest

from usr.plugins.durable.helpers.contract import (
    AgentStateSnapshot,
    CheckpointData,
    TaskState,
    TaskStatus,
    ToolIdempotencyKey,
)


def test_taskstate_roundtrip():
    s = TaskState(id="t1", context_snapshot={"k": "v"}, plan=["a"])
    s.transition_to(TaskStatus.PLANNED)
    s.transition_to(TaskStatus.EXECUTING)
    raw = s.to_dict()
    s2 = TaskState.from_dict(raw)
    assert s2.id == "t1"
    assert s2.status == TaskStatus.EXECUTING
    assert s2.context_snapshot == {"k": "v"}
    assert s2.plan == ["a"]
    assert s2.created_at == s.created_at


def test_taskstate_transition_terminal_guards():
    s = TaskState(id="t")
    s.transition_to(TaskStatus.COMPLETED)
    with pytest.raises(ValueError):
        s.transition_to(TaskStatus.EXECUTING)
    with pytest.raises(ValueError):
        s.transition_to(TaskStatus.FAILED)


def test_taskstate_created_can_execute():
    """Only terminal exits are guarded — CREATED → EXECUTING is legal."""
    s = TaskState(id="t")
    s.transition_to(TaskStatus.EXECUTING)
    assert s.status == TaskStatus.EXECUTING


def test_taskstate_touch_deterministic():
    from datetime import UTC, datetime

    s = TaskState(id="t")
    fixed = datetime(2026, 1, 1, tzinfo=UTC)
    s.touch(now=fixed)
    assert s.updated_at == fixed


def test_to_json_is_json_safe():
    import json

    s = TaskState(id="t", artifacts={"a": 1})
    assert json.loads(s.to_json())["id"] == "t"


def test_checkpoint_roundtrip():
    c = CheckpointData(
        iteration=3,
        prompt_messages=[{"role": "user", "content": "hi"}],
        llm_result={"tool_calls": []},
        tool_calls=[{"name": "x"}],
    )
    c2 = CheckpointData.from_dict(c.to_dict())
    assert c2.iteration == 3
    assert c2.llm_result == {"tool_calls": []}


def test_agent_state_snapshot_roundtrip():
    snap = AgentStateSnapshot(
        context={"id": "ctx"},
        history=[{"m": 1}],
        loop_data={"iteration": 2},
        data={"flags": []},
    )
    snap2 = AgentStateSnapshot.from_dict(snap.to_dict())
    assert snap2.context == {"id": "ctx"}
    assert snap2.loop_data == {"iteration": 2}


def test_idempotency_key_deterministic_sorted_args():
    k1 = ToolIdempotencyKey.build("tool", {"b": 2, "a": 1})
    k2 = ToolIdempotencyKey.build("tool", {"a": 1, "b": 2})
    assert k1 == k2
    name, _, digest = k1.partition(":")
    assert name == "tool" and len(digest) == 64


def test_idempotency_key_excludes_underscore_args():
    k1 = ToolIdempotencyKey.build("tool", {"a": 1})
    k2 = ToolIdempotencyKey.build("tool", {"a": 1, "_call": object()})
    assert k1 == k2


def test_idempotency_key_varies_by_name():
    assert ToolIdempotencyKey.build("a", {}) != ToolIdempotencyKey.build("b", {})


def test_idempotency_key_rejects_unserializable():
    with pytest.raises(TypeError):
        ToolIdempotencyKey.build("tool", {"cb": lambda: None})


def test_taskstatus_values_stable():
    """Statuses are journal keys — renaming breaks persisted state."""
    assert {s.value for s in TaskStatus} == {
        "created", "planned", "executing", "checkpointed",
        "paused", "resumed", "completed", "failed",
    }
