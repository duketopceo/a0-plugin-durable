"""Contract port coverage: TaskState lifecycle/serialization, CheckpointData,
AgentStateSnapshot, ToolIdempotencyKey determinism."""

import pytest

from usr.plugins.durable.helpers.contract import (
    AgentStateSnapshot,
    CheckpointData,
    TaskState,
    TaskStatus,
    ToolIdempotencyKey,
    iter_cap,
    json_or,
    loop_inputs,
    normalize_task_input,
    tool_message,
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


def test_normalize_task_input_drops_wire_state():
    """A submitted 'state' is never trusted — engines hydrate from their own
    journal, so wire state can't forge memoized steps or prompt history."""
    tid, data = normalize_task_input(
        {"id": "t1", "state": {"status": "completed", "tool_results": {"x": {}}}}
    )
    assert tid == "t1" and "state" not in data
    # state.id still resolves the task id before the payload is dropped
    tid2, data2 = normalize_task_input({"state": {"id": "t2"}, "k": 1})
    assert tid2 == "t2"
    assert data2 == {"id": "t2", "k": 1}


def test_normalize_task_input_rejects_bad_ids():
    for bad in ("../escape", "has space", "x" * 200, "sl@sh"):
        with pytest.raises(ValueError):
            normalize_task_input({"id": bad})
    # absent id mints a fresh one rather than raising
    tid, data = normalize_task_input({})
    assert tid and data["id"] == tid


def test_normalize_task_input_caps_size():
    with pytest.raises(ValueError, match="1 MiB"):
        normalize_task_input({"blob": "x" * (1 << 20)})


def test_iter_cap_defaults_and_clamps():
    assert iter_cap({}, 10) == 10
    assert iter_cap({"max_iterations": "bogus"}, 10) == 10
    assert iter_cap({"max_iterations": None}, 10) == 10
    assert iter_cap({"max_iterations": 0}, 10) == 0
    assert iter_cap({"max_iterations": -5}, 10) == 0
    # a submitter can tighten the loop but not request an unbounded one
    assert iter_cap({"max_iterations": 10**9}, 10) == 100


def test_loop_inputs_checkpoint_wins_over_raw_input():
    state = TaskState(
        id="t",
        context_snapshot={"prompt_messages": [{"m": "checkpointed"}]},
        model_state={"model_config": {"a": 1}},
    )
    msgs, cfg = loop_inputs(
        state, {"prompt_messages": [{"m": "raw"}], "model_config": {"b": 2}}
    )
    assert msgs == [{"m": "checkpointed"}]
    assert cfg == {"a": 1}


def test_tool_message_and_json_or():
    assert tool_message("w", {"result": "ok"}) == {
        "role": "tool", "name": "w", "content": "ok",
    }
    assert tool_message("w", {})["content"] == ""
    assert json_or(b'{"a": 1}') == {"a": 1}
    assert json_or("{corrupt", "d") == "d"
    assert json_or(None, "d") == "d"
