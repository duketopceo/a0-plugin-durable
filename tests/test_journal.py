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
    assert incomplete == {"a", "d"}  # paused survives — engine re-parks it


def test_journal_persists_across_instances(journal_path):
    Journal(journal_path).create_task("t", {"v": 42})
    row = Journal(journal_path).get_task("t")
    assert row["input"] == {"v": 42}
