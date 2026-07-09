"""Tests for cron schedule helpers."""

from src.core.cron_schedule import (
    compile_preset,
    describe_cron,
    next_run_times,
    resolve_schedule_input,
    validate_cron_expr,
)


def test_compile_daily() -> None:
    assert compile_preset({"type": "daily", "time": "08:00"}) == "0 8 * * *"
    assert compile_preset({"type": "daily", "hour": 9, "minute": 30}) == "30 9 * * *"


def test_compile_weekly() -> None:
    expr = compile_preset(
        {"type": "weekly", "time": "09:30", "weekdays": ["mon", "wed", "fri"]}
    )
    assert expr == "30 9 * * mon,wed,fri"


def test_compile_every_minutes() -> None:
    assert compile_preset({"type": "every_minutes", "interval": 15}) == "*/15 * * * *"


def test_compile_monthly() -> None:
    assert compile_preset({"type": "monthly", "time": "10:00", "day_of_month": 1}) == "0 10 1 * *"


def test_validate_rejects_bad() -> None:
    try:
        validate_cron_expr("not a cron")
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_describe_daily() -> None:
    label = describe_cron("0 8 * * *", timezone="Asia/Shanghai")
    assert "每天" in label
    assert "08:00" in label


def test_next_run_times_count() -> None:
    runs = next_run_times("0 8 * * *", count=3, timezone="Asia/Shanghai")
    assert len(runs) == 3
    assert all("T" in r or "+" in r for r in runs)


def test_resolve_preset_schedule() -> None:
    resolved = resolve_schedule_input(
        schedule={"mode": "preset", "preset": {"type": "daily", "time": "08:00"}},
        timezone="Asia/Shanghai",
    )
    assert resolved["cron_expr"] == "0 8 * * *"
    assert resolved["human_label"]
    assert resolved["schedule_meta"]["mode"] == "preset"


def test_resolve_raw_cron() -> None:
    resolved = resolve_schedule_input(cron_expr="0 8 * * *")
    assert resolved["cron_expr"] == "0 8 * * *"
