"""Tests for Task model lifecycle and methods."""

from datetime import datetime

from src.core.task import Task, TaskTarget, TaskStatus, TaskPriority, TaskStepLog


class TestTaskCreation:
    def test_defaults(self):
        t = Task(name="Test")
        assert t.name == "Test"
        assert t.status == TaskStatus.PENDING
        assert t.progress == 0.0
        assert t.targets == []
        assert t.config == {}
        assert t.priority == TaskPriority.NORMAL
        assert t.retry_count == 0
        assert t.max_retries == 3
        assert t.error is None
        assert t.result is None
        assert t.cron_expr is None
        assert isinstance(t.id, str) and len(t.id) == 12
        assert isinstance(t.created_at, datetime)

    def test_with_targets(self):
        targets = [TaskTarget(name="CS2", target_type="game", params={"app_id": "730"})]
        t = Task(name="Collect", targets=targets, collector_name="steam")
        assert len(t.targets) == 1
        assert t.targets[0].name == "CS2"
        assert t.targets[0].params["app_id"] == "730"
        assert t.collector_name == "steam"

    def test_with_config(self):
        t = Task(name="T", config={"refresh": {"rolling_window": True}})
        assert t.config["refresh"]["rolling_window"] is True


class TestTaskTarget:
    def test_default_type(self):
        tt = TaskTarget(name="test")
        assert tt.target_type == "default"

    def test_params_default_empty(self):
        tt = TaskTarget(name="x")
        assert tt.params == {}


class TestTaskLifecycle:
    def test_start(self):
        t = Task(name="T")
        t.start()
        assert t.status == TaskStatus.RUNNING
        assert t.started_at is not None
        assert t.progress == 0.0

    def test_complete(self):
        t = Task(name="T")
        t.start()
        t.complete({"storage_count": 5})
        assert t.status == TaskStatus.SUCCESS
        assert t.progress == 1.0
        assert t.result == {"storage_count": 5}
        assert t.completed_at is not None

    def test_fail(self):
        t = Task(name="T")
        t.start()
        t.fail("timeout")
        assert t.status == TaskStatus.FAILED
        assert t.error == "timeout"
        assert t.completed_at is not None

    def test_cancel(self):
        t = Task(name="T")
        t.start()
        t.cancel()
        assert t.status == TaskStatus.CANCELLED
        assert t.completed_at is not None

    def test_retry_under_limit(self):
        t = Task(name="T")
        t.start()
        t.fail("err")
        assert t.retry() is True
        assert t.status == TaskStatus.RETRYING
        assert t.retry_count == 1
        assert t.error is None
        assert t.started_at is not None

    def test_retry_exhausted(self):
        t = Task(name="T", max_retries=0)
        assert t.retry() is False
        assert t.retry_count == 0

    def test_retry_at_limit(self):
        t = Task(name="T", max_retries=1)
        t.start()
        t.fail("err")
        assert t.retry() is True
        t.fail("err")
        assert t.retry_count == 1
        assert t.retry() is False
        assert t.retry_count == 1


class TestTaskProperties:
    def test_is_terminal(self):
        t = Task(name="T")
        assert t.is_terminal is False
        t.start()
        assert t.is_terminal is False
        t.complete()
        assert t.is_terminal is True

    def test_is_terminal_failed(self):
        t = Task(name="T")
        t.fail("x")
        assert t.is_terminal is True

    def test_is_terminal_cancelled(self):
        t = Task(name="T")
        t.cancel()
        assert t.is_terminal is True

    def test_duration_none_before_start(self):
        t = Task(name="T")
        assert t.duration_seconds is None

    def test_duration_after_start(self):
        t = Task(name="T")
        t.start()
        assert t.duration_seconds is not None
        assert t.duration_seconds >= 0


class TestTaskProgress:
    def test_update_progress(self):
        t = Task(name="T")
        t.start()
        t.update_progress(0.5, "half done")
        assert t.progress == 0.5

    def test_update_progress_clamps_low(self):
        t = Task(name="T")
        t.update_progress(-0.5)
        assert t.progress == 0.0

    def test_update_progress_clamps_high(self):
        t = Task(name="T")
        t.update_progress(1.5)
        assert t.progress == 1.0

    def test_update_progress_adds_log(self):
        t = Task(name="T")
        t.update_progress(0.3, "step 1")
        assert len(t.step_logs) == 1
        assert t.step_logs[0].message == "step 1"

    def test_update_progress_no_log_if_no_message(self):
        t = Task(name="T")
        t.update_progress(0.3)
        assert len(t.step_logs) == 0


class TestTaskStepLog:
    def test_add_step_log(self):
        t = Task(name="T")
        t.add_step_log("collect:steam", TaskStatus.RUNNING, "starting")
        assert len(t.step_logs) == 1
        assert t.step_logs[0].step_name == "collect:steam"
        assert t.step_logs[0].status == TaskStatus.RUNNING

    def test_add_step_log_with_error(self):
        t = Task(name="T")
        t.add_step_log("collect:steam", TaskStatus.FAILED, "failed", error="timeout")
        assert t.step_logs[0].error == "timeout"


class TestTaskSummary:
    def test_to_summary(self):
        t = Task(
            name="My Task",
            collector_name="steam",
            targets=[TaskTarget(name="CS2", params={"app_id": "730"})],
        )
        s = t.to_summary()
        assert s["name"] == "My Task"
        assert s["status"] == "pending"
        assert s["progress"] == 0.0
        assert s["collector"] == "steam"
        assert s["targets_count"] == 1
        assert "created_at" in s
        assert s["duration"] is None

    def test_to_summary_after_complete(self, task_completed):
        s = task_completed.to_summary()
        assert s["status"] == "success"
        assert s["progress"] == 1.0


class TestTaskStorageRoundtrip:
    def test_roundtrip(self, task_completed):
        payload = task_completed.to_storage_payload()
        restored = Task.from_storage_payload(payload)
        assert restored.id == task_completed.id
        assert restored.name == task_completed.name
        assert restored.status == task_completed.status
        assert restored.result is None  # result excluded from storage

    def test_result_summary(self, task_completed):
        summary = task_completed.result_summary
        assert summary is not None
        assert summary.get("success") is True
        assert summary.get("storage_count") == 10

    def test_result_summary_none_when_no_result(self):
        t = Task(name="T")
        assert t.result_summary is None


class TestTaskFromStorage:
    def test_restores_pending(self):
        t = Task(name="T")
        payload = t.to_storage_payload()
        restored = Task.from_storage_payload(payload)
        assert restored.status == TaskStatus.PENDING

    def test_restores_failed_with_error(self):
        t = Task(name="T")
        t.fail("boom")
        payload = t.to_storage_payload()
        restored = Task.from_storage_payload(payload)
        assert restored.status == TaskStatus.FAILED
        assert restored.error == "boom"


class TestStepLogModel:
    def test_defaults(self):
        sl = TaskStepLog(step_name="test")
        assert sl.step_name == "test"
        assert sl.status == TaskStatus.PENDING
        assert sl.message == ""
        assert sl.error is None
