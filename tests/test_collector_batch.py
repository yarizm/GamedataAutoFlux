import asyncio

import pytest

from src.collectors.base import BaseCollector, CollectResult, CollectTarget, _sleep_before_retry
from src.collectors.gtrends_collector import GoogleTrendsCollector


class DummyCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        delay = float(target.params.get("delay", 0))
        if delay:
            await asyncio.sleep(delay)
        if target.params.get("raise"):
            raise TimeoutError("request timeout")
        return CollectResult(target=target, data={"name": target.name})


class TrackingCollector(BaseCollector):
    def __init__(self, config=None):
        super().__init__(config)
        self.active = 0
        self.max_active = 0

    async def collect(self, target: CollectTarget) -> CollectResult:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(float(target.params.get("delay", 0)))
            return CollectResult(target=target, data={"name": target.name})
        finally:
            self.active -= 1


class TapTapCollector(TrackingCollector):
    pass


class FlakyCollector(BaseCollector):
    def __init__(self, config=None):
        super().__init__(config)
        self.attempts: dict[str, int] = {}

    async def collect(self, target: CollectTarget) -> CollectResult:
        attempt = self.attempts.get(target.name, 0) + 1
        self.attempts[target.name] = attempt
        if attempt <= int(target.params.get("failures", 0)):
            raise TimeoutError("temporary network timeout")
        return CollectResult(
            target=target,
            data={"name": target.name, "attempt": attempt},
            metadata={"collector": "flaky"},
        )


class ReturnedFailureCollector(BaseCollector):
    def __init__(self, config=None):
        super().__init__(config)
        self.attempts = 0

    async def collect(self, target: CollectTarget) -> CollectResult:
        self.attempts += 1
        if self.attempts <= int(target.params.get("failures", 0)):
            return CollectResult(
                target=target,
                success=False,
                error=target.params.get("error", "HTTP 429 Too Many Requests"),
                error_code=target.params.get("error_code", "rate_limited"),
                metadata={"source": "returned"},
            )
        return CollectResult(
            target=target,
            data={"name": target.name, "attempt": self.attempts},
            metadata={"source": "returned"},
        )


class OverridingFailureCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(
            target=target,
            success=False,
            error="no data found",
            error_code="empty_data",
            metadata={
                "collector": "unsafe",
                "target_params": {"api_key": "raw-secret"},
                "error_code": "unknown",
                "source": "returned",
            },
        )


class SensitiveMetadataCollector(BaseCollector):
    async def collect(self, target: CollectTarget) -> CollectResult:
        return CollectResult(
            target=target,
            data={"name": target.name},
            metadata={
                "api_key": "raw-secret",
                "nested": {"token": "secret-token"},
                "authorization": "Bearer abcdefghijklmnop",
            },
        )


@pytest.mark.asyncio
async def test_collect_batch_preserves_target_order_with_concurrency() -> None:
    collector = DummyCollector({"batch_concurrency": 2})
    targets = [
        CollectTarget(name="slow", params={"delay": 0.02}),
        CollectTarget(name="fast", params={}),
    ]

    results = await collector.collect_batch(targets)

    assert [result.target.name for result in results] == ["slow", "fast"]
    assert all(result.success for result in results)


@pytest.mark.asyncio
async def test_collect_batch_classifies_errors() -> None:
    collector = DummyCollector({"batch_concurrency": 2, "collector_name": "dummy"})
    targets = [
        CollectTarget(name="ok"),
        CollectTarget(name="bad", params={"raise": True, "api_key": "secret"}),
    ]

    results = await collector.collect_batch(targets)

    assert results[0].success is True
    assert results[1].success is False
    assert results[1].error_code == "network_unreachable"
    assert results[1].metadata == {
        "collector": "dummy",
        "target": "bad",
        "target_type": "default",
        "target_params": {"raise": True, "api_key": "[REDACTED]"},
        "error_code": "network_unreachable",
    }


@pytest.mark.asyncio
async def test_collect_batch_enforces_explicit_collect_timeout() -> None:
    collector = DummyCollector(
        {
            "collector_name": "dummy",
            "batch_concurrency": 1,
            "collect_timeout": 0.01,
        }
    )

    results = await collector.collect_batch(
        [CollectTarget(name="too-slow", params={"delay": 0.05, "api_key": "secret"})]
    )

    assert results[0].success is False
    assert results[0].error == "Collect timeout after 0.01s"
    assert results[0].error_code == "network_unreachable"
    assert results[0].metadata == {
        "collector": "dummy",
        "target": "too-slow",
        "target_type": "default",
        "target_params": {"delay": 0.05, "api_key": "[REDACTED]"},
        "error_code": "network_unreachable",
        "collect_timeout": 0.01,
    }


@pytest.mark.asyncio
async def test_collect_batch_retries_retryable_exception() -> None:
    collector = FlakyCollector(
        {
            "collector_name": "flaky",
            "collect_retries": 2,
            "collect_retry_delay": 0,
        }
    )

    results = await collector.collect_batch(
        [CollectTarget(name="eventual", params={"failures": 1})]
    )

    assert results[0].success is True
    assert results[0].data == {"name": "eventual", "attempt": 2}
    assert results[0].metadata == {
        "collector": "flaky",
        "attempts": 2,
        "max_attempts": 3,
        "retry_attempts": 1,
        "last_retry_error": "temporary network timeout",
        "last_retry_error_code": "network_unreachable",
    }
    assert results[0].to_summary()["retry"] == {
        "attempts": 2,
        "max_attempts": 3,
        "retry_attempts": 1,
        "last_retry_error": "temporary network timeout",
        "last_retry_error_code": "network_unreachable",
    }


@pytest.mark.asyncio
async def test_collect_batch_records_retry_metadata_after_exhaustion() -> None:
    collector = FlakyCollector(
        {
            "collector_name": "flaky",
            "collect_retries": 1,
            "collect_retry_delay": 0,
        }
    )

    results = await collector.collect_batch(
        [CollectTarget(name="always-bad", params={"failures": 99})]
    )

    assert results[0].success is False
    assert results[0].error_code == "network_unreachable"
    assert results[0].metadata == {
        "collector": "flaky",
        "target": "always-bad",
        "target_type": "default",
        "target_params": {"failures": 99},
        "error_code": "network_unreachable",
        "attempts": 2,
        "max_attempts": 2,
        "retry_attempts": 1,
        "last_retry_error": "temporary network timeout",
        "last_retry_error_code": "network_unreachable",
    }
    assert results[0].to_summary()["retry"] == {
        "attempts": 2,
        "max_attempts": 2,
        "retry_attempts": 1,
        "last_retry_error": "temporary network timeout",
        "last_retry_error_code": "network_unreachable",
    }


@pytest.mark.asyncio
async def test_collect_batch_records_retry_metadata_for_first_attempt_success() -> None:
    collector = FlakyCollector(
        {
            "collector_name": "flaky",
            "collect_retries": 2,
            "collect_retry_delay": 0,
        }
    )

    results = await collector.collect_batch([CollectTarget(name="stable")])

    assert results[0].success is True
    assert results[0].metadata == {
        "collector": "flaky",
        "attempts": 1,
        "max_attempts": 3,
        "retry_attempts": 0,
    }
    assert results[0].to_summary()["retry"] == {
        "attempts": 1,
        "max_attempts": 3,
        "retry_attempts": 0,
    }


@pytest.mark.asyncio
async def test_collect_batch_retries_returned_retryable_failure() -> None:
    collector = ReturnedFailureCollector(
        {
            "collector_name": "returned",
            "collect_retries": 1,
            "collect_retry_delay": 0,
        }
    )

    results = await collector.collect_batch([CollectTarget(name="limited", params={"failures": 1})])

    assert collector.attempts == 2
    assert results[0].success is True
    assert results[0].data == {"name": "limited", "attempt": 2}
    assert results[0].metadata == {
        "source": "returned",
        "attempts": 2,
        "max_attempts": 2,
        "retry_attempts": 1,
        "last_retry_error": "HTTP 429 Too Many Requests",
        "last_retry_error_code": "rate_limited",
    }
    assert results[0].to_summary()["retry"]["last_retry_error"] == "HTTP 429 Too Many Requests"


@pytest.mark.asyncio
async def test_collect_batch_does_not_retry_returned_non_retryable_failure() -> None:
    collector = ReturnedFailureCollector(
        {
            "collector_name": "returned",
            "collect_retries": 2,
            "collect_retry_delay": 0,
        }
    )

    results = await collector.collect_batch(
        [
            CollectTarget(
                name="empty",
                params={
                    "failures": 1,
                    "error": "no data found",
                    "error_code": "empty_data",
                },
            )
        ]
    )

    assert collector.attempts == 1
    assert results[0].success is False
    assert results[0].error_code == "empty_data"
    assert results[0].metadata == {
        "collector": "returned",
        "target": "empty",
        "target_type": "default",
        "target_params": {
            "failures": 1,
            "error": "no data found",
            "error_code": "empty_data",
        },
        "error_code": "empty_data",
        "attempts": 1,
        "max_attempts": 3,
        "retry_attempts": 0,
        "source": "returned",
    }


@pytest.mark.asyncio
async def test_collect_batch_failure_metadata_cannot_override_redacted_fields() -> None:
    collector = OverridingFailureCollector({"collector_name": "safe"})

    results = await collector.collect_batch(
        [CollectTarget(name="unsafe", params={"api_key": "raw-secret"})]
    )

    assert results[0].success is False
    assert results[0].error_code == "empty_data"
    assert results[0].metadata == {
        "collector": "safe",
        "target_params": {"api_key": "[REDACTED]"},
        "error_code": "empty_data",
        "source": "returned",
        "target": "unsafe",
        "target_type": "default",
    }


@pytest.mark.asyncio
async def test_collect_batch_redacts_success_metadata() -> None:
    collector = SensitiveMetadataCollector()

    results = await collector.collect_batch([CollectTarget(name="safe")])

    assert results[0].success is True
    assert results[0].metadata == {
        "api_key": "[REDACTED]",
        "nested": {"token": "[REDACTED]"},
        "authorization": "[REDACTED]",
    }


def test_collect_result_retry_summary_redacts_last_retry_error() -> None:
    result = CollectResult(
        target=CollectTarget(name="safe"),
        metadata={
            "attempts": 2,
            "max_attempts": 3,
            "retry_attempts": 1,
            "last_retry_error": "retry failed token=secret-token",
            "last_retry_error_code": "rate_limited",
        },
    )

    summary = result.to_summary()

    assert summary["retry"] == {
        "attempts": 2,
        "max_attempts": 3,
        "retry_attempts": 1,
        "last_retry_error": "retry failed token=[REDACTED]",
        "last_retry_error_code": "rate_limited",
    }


@pytest.mark.asyncio
async def test_collect_retry_log_redacts_target_and_error(monkeypatch) -> None:
    captured = {}

    def fake_warning(message, *args):
        captured["message"] = message
        captured["args"] = args

    monkeypatch.setattr("src.collectors.base.logger.warning", fake_warning)

    await _sleep_before_retry(
        DummyCollector({"collector_name": "dummy"}),
        CollectTarget(name="target api_key=target-secret"),
        attempt=1,
        retry_delay=0,
        error="retry token=error-secret",
    )

    rendered = " ".join(str(arg) for arg in captured["args"])
    assert "target-secret" not in rendered
    assert "error-secret" not in rendered
    assert "api_key=[REDACTED]" in rendered
    assert "token=[REDACTED]" in rendered


@pytest.mark.asyncio
async def test_collect_batch_uses_global_concurrency_default(monkeypatch) -> None:
    import src.core.config as config_module

    def fake_get(key: str, default=None):
        if key == "collector.batch_concurrency":
            return 2
        return default

    monkeypatch.setattr(config_module, "get", fake_get)
    collector = TrackingCollector()
    targets = [
        CollectTarget(name="one", params={"delay": 0.02}),
        CollectTarget(name="two", params={"delay": 0.02}),
    ]

    await collector.collect_batch(targets)

    assert collector.max_active == 2


@pytest.mark.asyncio
async def test_collect_batch_uses_global_collect_timeout_default(monkeypatch) -> None:
    import src.core.config as config_module

    def fake_get(key: str, default=None):
        if key == "collector.collect_timeout":
            return 0.01
        return default

    monkeypatch.setattr(config_module, "get", fake_get)
    collector = DummyCollector({"collector_name": "dummy"})

    results = await collector.collect_batch([CollectTarget(name="slow", params={"delay": 0.05})])

    assert results[0].success is False
    assert results[0].metadata["collect_timeout"] == 0.01


@pytest.mark.asyncio
async def test_collect_batch_explicit_zero_collect_timeout_disables_global_default(
    monkeypatch,
) -> None:
    import src.core.config as config_module

    def fake_get(key: str, default=None):
        if key == "collector.collect_timeout":
            return 0.01
        return default

    monkeypatch.setattr(config_module, "get", fake_get)
    collector = DummyCollector({"collector_name": "dummy", "collect_timeout": 0})

    results = await collector.collect_batch([CollectTarget(name="slow", params={"delay": 0.02})])

    assert results[0].success is True


@pytest.mark.asyncio
async def test_collect_batch_uses_per_collector_concurrency_default(monkeypatch) -> None:
    import src.core.config as config_module

    def fake_get(key: str, default=None):
        if key == "collector.batch_concurrency":
            return 1
        if key == "taptap.batch_concurrency":
            return 3
        return default

    monkeypatch.setattr(config_module, "get", fake_get)
    collector = TapTapCollector()
    targets = [
        CollectTarget(name="one", params={"delay": 0.02}),
        CollectTarget(name="two", params={"delay": 0.02}),
        CollectTarget(name="three", params={"delay": 0.02}),
    ]

    await collector.collect_batch(targets)

    assert collector.max_active == 3


@pytest.mark.asyncio
async def test_collect_batch_per_collector_zero_collect_timeout_disables_global_default(
    monkeypatch,
) -> None:
    import src.core.config as config_module

    def fake_get(key: str, default=None):
        if key == "collector.collect_timeout":
            return 0.01
        if key == "taptap.collect_timeout":
            return 0
        return default

    monkeypatch.setattr(config_module, "get", fake_get)
    collector = TapTapCollector()

    results = await collector.collect_batch([CollectTarget(name="slow", params={"delay": 0.02})])

    assert results[0].success is True


@pytest.mark.asyncio
async def test_collect_batch_config_overrides_per_collector_default(monkeypatch) -> None:
    import src.core.config as config_module

    def fake_get(key: str, default=None):
        if key == "collector.batch_concurrency":
            return 1
        if key == "taptap.batch_concurrency":
            return 3
        return default

    monkeypatch.setattr(config_module, "get", fake_get)
    collector = TapTapCollector({"batch_concurrency": 2})
    targets = [
        CollectTarget(name="one", params={"delay": 0.02}),
        CollectTarget(name="two", params={"delay": 0.02}),
        CollectTarget(name="three", params={"delay": 0.02}),
    ]

    await collector.collect_batch(targets)

    assert collector.max_active == 2


@pytest.mark.asyncio
async def test_gtrends_collect_batch_classifies_exception_metadata() -> None:
    collector = GoogleTrendsCollector({"collector_name": "gtrends"})

    async def fail_collect(target: CollectTarget) -> CollectResult:
        raise RuntimeError("HTTP 429 Too Many Requests")

    collector.collect = fail_collect

    results = await collector.collect_batch(
        [CollectTarget(name="blocked", params={"api_key": "secret"})]
    )

    assert results[0].success is False
    assert results[0].error_code == "rate_limited"
    assert results[0].metadata == {
        "collector": "gtrends",
        "target": "blocked",
        "target_type": "default",
        "target_params": {"api_key": "[REDACTED]"},
        "error_code": "rate_limited",
    }


@pytest.mark.asyncio
async def test_gtrends_collect_batch_enriches_failed_results() -> None:
    collector = GoogleTrendsCollector({"collector_name": "gtrends"})

    async def failed_result(target: CollectTarget) -> CollectResult:
        return CollectResult(
            target=target,
            success=False,
            error="missing keyword token=secret-token",
            error_code="empty_data",
            metadata={
                "data_sources": ["pytrends(failed)"],
                "target_params": {"token": "raw-secret"},
                "error_code": "unknown",
            },
        )

    collector.collect = failed_result

    results = await collector.collect_batch(
        [CollectTarget(name="empty", params={"token": "secret"})]
    )

    assert results[0].success is False
    assert results[0].error == "missing keyword token=[REDACTED]"
    assert results[0].metadata == {
        "collector": "gtrends",
        "target": "empty",
        "target_type": "default",
        "target_params": {"token": "[REDACTED]"},
        "error_code": "empty_data",
        "data_sources": ["pytrends(failed)"],
    }


@pytest.mark.asyncio
async def test_gtrends_collect_batch_enforces_collect_timeout() -> None:
    collector = GoogleTrendsCollector({"collector_name": "gtrends", "collect_timeout": 0.01})

    async def slow_collect(target: CollectTarget) -> CollectResult:
        await asyncio.sleep(0.05)
        return CollectResult(target=target, data={"name": target.name})

    collector.collect = slow_collect

    results = await collector.collect_batch([CollectTarget(name="slow")])

    assert results[0].success is False
    assert results[0].error_code == "network_unreachable"
    assert results[0].metadata["collect_timeout"] == 0.01


@pytest.mark.asyncio
async def test_gtrends_collect_batch_retries_rate_limited_exception() -> None:
    collector = GoogleTrendsCollector(
        {
            "collector_name": "gtrends",
            "collect_retries": 1,
            "collect_retry_delay": 0,
        }
    )
    attempts = 0

    async def flaky_collect(target: CollectTarget) -> CollectResult:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("HTTP 429 Too Many Requests")
        return CollectResult(target=target, data={"name": target.name}, metadata={"ok": True})

    collector.collect = flaky_collect

    results = await collector.collect_batch([CollectTarget(name="limited")])

    assert results[0].success is True
    assert attempts == 2
    assert results[0].metadata == {
        "ok": True,
        "attempts": 2,
        "max_attempts": 2,
        "retry_attempts": 1,
        "last_retry_error": "HTTP 429 Too Many Requests",
        "last_retry_error_code": "rate_limited",
    }


@pytest.mark.asyncio
async def test_gtrends_collect_batch_records_retry_metadata_after_exhaustion() -> None:
    collector = GoogleTrendsCollector(
        {
            "collector_name": "gtrends",
            "collect_retries": 1,
            "collect_retry_delay": 0,
        }
    )

    async def fail_collect(target: CollectTarget) -> CollectResult:
        raise RuntimeError("HTTP 429 Too Many Requests password=supersecret")

    collector.collect = fail_collect

    results = await collector.collect_batch([CollectTarget(name="limited")])

    assert results[0].success is False
    assert results[0].error_code == "rate_limited"
    assert results[0].metadata == {
        "collector": "gtrends",
        "target": "limited",
        "target_type": "default",
        "error_code": "rate_limited",
        "attempts": 2,
        "max_attempts": 2,
        "retry_attempts": 1,
        "last_retry_error": "HTTP 429 Too Many Requests password=[REDACTED]",
        "last_retry_error_code": "rate_limited",
    }
    assert results[0].to_summary()["retry"] == {
        "attempts": 2,
        "max_attempts": 2,
        "retry_attempts": 1,
        "last_retry_error": "HTTP 429 Too Many Requests password=[REDACTED]",
        "last_retry_error_code": "rate_limited",
    }


@pytest.mark.asyncio
async def test_gtrends_collect_batch_retries_returned_network_failure() -> None:
    collector = GoogleTrendsCollector(
        {
            "collector_name": "gtrends",
            "collect_retries": 1,
            "collect_retry_delay": 0,
        }
    )
    attempts = 0

    async def returned_failure_then_success(target: CollectTarget) -> CollectResult:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return CollectResult(
                target=target,
                success=False,
                error="connection timeout",
                error_code="network_unreachable",
                metadata={"source": "returned"},
            )
        return CollectResult(target=target, data={"name": target.name}, metadata={"ok": True})

    collector.collect = returned_failure_then_success

    results = await collector.collect_batch([CollectTarget(name="network")])

    assert results[0].success is True
    assert attempts == 2
    assert results[0].metadata == {
        "ok": True,
        "attempts": 2,
        "max_attempts": 2,
        "retry_attempts": 1,
        "last_retry_error": "connection timeout",
        "last_retry_error_code": "network_unreachable",
    }


@pytest.mark.asyncio
async def test_gtrends_collect_batch_resumes_from_checkpoint() -> None:
    collector = GoogleTrendsCollector(
        {
            "collector_name": "gtrends",
            "recovery_checkpoint": {
                "checkpoint_id": "checkpoint-1",
                "recovery_level": "L1",
                "collect": {
                    "enabled": True,
                    "next_target_index": 2,
                    "target_order": ["A", "B", "C"],
                },
            },
        }
    )
    collected_targets: list[str] = []

    async def collect(target: CollectTarget) -> CollectResult:
        collected_targets.append(target.name)
        return CollectResult(
            target=target,
            data={"name": target.name},
            metadata={"collector": "gtrends"},
        )

    collector.collect = collect

    results = await collector.collect_batch(
        [
            CollectTarget(name="A"),
            CollectTarget(name="B"),
            CollectTarget(name="C"),
        ]
    )

    assert collected_targets == ["C"]
    assert [result.target.name for result in results] == ["C"]
    assert results[0].metadata["resume"] == {
        "resumed": True,
        "checkpoint_id": "checkpoint-1",
        "recovery_level": "L1",
        "target_index": 2,
        "target": "C",
    }
