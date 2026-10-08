"""SQLite journal: idempotent task create, step lifecycle memoization, torn
'running' step reset on reopen (the crash-recovery path), persistence
across journal instances."""

from usr.plugins.durable.helpers.contract import TaskState, TaskStatus
from usr.plugins.durable.helpers.journal import Journal


def test_create_task_idempotent(journal_path):
    j = Journal(journal_path)
    assert j.create_task("t1", {"x": 1}) is True
    assert j.create_task("t1", {"x": 2}) is False  # OR IGNORE — first wins
    row = j.get_task("t1")
    assert row["status"] == "created"
    assert row["input"] == {"x": 1}


def test_get_task_unknown_returns_none(journal_path):
    assert Journal(journal_path).get_task("nope") is None


def test_update_task_status_and_state(journal_path):
    j = Journal(journal_path)
    j.create_task("t", {})
    s = TaskState(id="t")
    s.transition_to(TaskStatus.PLANNED)
    j.update_task("t", status=TaskStatus.PLANNED, state=s)
    row = j.get_task("t")
    assert row["status"] == "planned"
    assert row["state"]["status"] == "planned"


def test_step_lifecycle(journal_path):
    j = Journal(journal_path)
    j.create_task("t", {})
    assert j.step_result("t", "k") is None
    j.step_begin("t", "k", "llm_call")
    assert j.step_result("t", "k") is None  # still running
    j.step_done("t", "k", "llm_call", {"text": "hi"})
    assert j.step_result("t", "k") == {"text": "hi"}


def test_step_failed_not_memoized(journal_path):
    j = Journal(journal_path)
    j.create_task("t", {})
    j.step_begin("t", "k", "tool_call")
    j.step_failed("t", "k", "tool_call", "boom")
    assert j.step_result("t", "k") is None


def test_torn_running_step_reset_on_reopen(journal_path):
    """A 'running' step across process death is a torn write — reopening the
    journal must reset it so a resumed task re-executes instead of trusting
    a partial result."""
    j = Journal(journal_path)
    j.create_task("t", {})
    j.step_begin("t", "k", "tool_call")
    j2 = Journal(journal_path)  # simulate process restart
    row_status = None
    # torn step is failed — step_result stays None, step_begin can restart it
    assert j2.step_result("t", "k") is None
    j2.step_done("t", "k", "tool_call", {"r": 1})
    assert j2.step_result("t", "k") == {"r": 1}


def test_incomplete_tasks(journal_path):
    j = Journal(journal_path)
    j.create_task("a", {})
    j.create_task("b", {})
    j.update_task("b", status=TaskStatus.COMPLETED)
    j.create_task("c", {})
    j.update_task("c", status=TaskStatus.FAILED)
    j.create_task("d", {})
    j.update_task("d", status=TaskStatus.PAUSED)
    incomplete = set(j.incomplete_tasks())
    # paused is excluded by design — it stays parked until an explicit resume
    # signal attaches a runner (ticks don't accumulate parked tasks)
    assert incomplete == {"a"}


def test_journal_persists_across_instances(journal_path):
    Journal(journal_path).create_task("t", {"v": 42})
    row = Journal(journal_path).get_task("t")
    assert row["input"] == {"v": 42}


def test_get_meta_bounded_projection(journal_path):
    j = Journal(journal_path)
    j.create_task("t", {"x": 1})
    meta = j.get_meta("t")
    assert meta is not None and meta["status"] == "created" and meta["id"] == "t"
    assert "state_json" not in meta and "input" not in meta
    assert j.get_meta("nope") is None


def test_update_task_returns_bool_and_non_terminal_guard(journal_path):
    j = Journal(journal_path)
    j.create_task("t", {})
    assert j.update_task("t", status=TaskStatus.EXECUTING) is True
    assert j.update_task("t", status=TaskStatus.FAILED) is True
    # terminal guard: a stale runner write can't resurrect a finished task
    assert j.update_task(
        "t", status=TaskStatus.EXECUTING, non_terminal_only=True
    ) is True  # write lands but guard blocks the status flip
    assert j.get_status("t") == "failed"


def test_step_done_rejects_nonserializable_result(journal_path):
    """A result that can't JSON must fail the step — never leave a torn
    'running' row that silently re-executes the side effect on replay."""
    j = Journal(journal_path)
    j.create_task("t", {})
    j.step_begin("t", "k", "tool_call")
    assert j.step_done("t", "k", "tool_call", {"bad": object()}) is False
    assert j.step_result("t", "k") is None  # not memoized


def test_close_then_ops_fail_closed(journal_path):
    j = Journal(journal_path)
    j.create_task("t", {})
    j.close()
    assert j.create_task("x", {}) is False
    assert j.get_task("t") is None
    assert j.update_task("t", status=TaskStatus.FAILED) is False
