"""Tests for daily report metrics, formatting, scheduler, and QuickChart URL."""

from datetime import datetime, timedelta, timezone

import pytest

from daily_report import (
    build_narrative,
    build_quickchart_url,
    format_report_html,
)
from health import HealthCheckResult
from metrics import (
    DailyReportMetrics,
    DestinationStats,
    QueueSnapshot,
    ComponentStatus,
    compute_verdict,
    progress_bar,
    sparkline,
)
from scheduler import seconds_until_next_run

UTC_PLUS_3 = timezone(timedelta(hours=3))

def _ok_checks():
    return [
        HealthCheckResult(name="pyrogram_session", passed=True, level="critical"),
        HealthCheckResult(name="sender_bot", passed=True, level="critical"),
        HealthCheckResult(name="redis", passed=True, level="critical"),
        HealthCheckResult(name="sqlite", passed=True, level="critical"),
        HealthCheckResult(name="n8n_webhook", passed=True, level="high"),
        HealthCheckResult(name="disk_space", passed=True, level="high"),
    ]


class TestVerdict:
    def test_green_when_healthy(self):
        q = QueueSnapshot(pending=0, retry=0, dead_letter=0)
        assert compute_verdict(_ok_checks(), q, 98.0, True) == "green"

    def test_yellow_on_retry_queue(self):
        q = QueueSnapshot(pending=0, retry=3, dead_letter=0)
        assert compute_verdict(_ok_checks(), q, 99.0, True) == "yellow"

    def test_yellow_on_dlq(self):
        q = QueueSnapshot(pending=0, retry=0, dead_letter=1)
        assert compute_verdict(_ok_checks(), q, 99.0, True) == "yellow"

    def test_yellow_on_mid_success(self):
        q = QueueSnapshot()
        assert compute_verdict(_ok_checks(), q, 90.0, True) == "yellow"

    def test_red_on_critical(self):
        checks = _ok_checks()
        checks[1] = HealthCheckResult(
            name="sender_bot", passed=False, level="critical", message="down"
        )
        q = QueueSnapshot()
        assert compute_verdict(checks, q, 100.0, True) == "red"

    def test_red_on_low_success(self):
        q = QueueSnapshot()
        assert compute_verdict(_ok_checks(), q, 70.0, True) == "red"

    def test_red_on_deep_pending(self):
        q = QueueSnapshot(pending=100)
        assert compute_verdict(_ok_checks(), q, 100.0, True) == "red"

    def test_green_with_no_attempts_ignores_rate(self):
        q = QueueSnapshot()
        # rate would look bad if blindly applied, but no traffic → green
        assert compute_verdict(_ok_checks(), q, 0.0, False) == "green"


class TestProgressBar:
    def test_full(self):
        assert progress_bar(100) == "██████████ 100%"

    def test_partial(self):
        bar = progress_bar(94)
        assert "94%" in bar
        assert "█" in bar
        assert "░" in bar

    def test_zero(self):
        assert progress_bar(0).startswith("░" * 10)


class TestSparkline:
    def test_empty(self):
        assert sparkline([]) == "—"

    def test_all_zero(self):
        assert sparkline([0, 0, 0]) == "▁▁▁"

    def test_trend_length(self):
        s = sparkline([10, 20, 30, 40, 50, 60, 70])
        assert len(s) == 7

    def test_flat(self):
        s = sparkline([5, 5, 5])
        assert len(s) == 3


class TestQuickChart:
    def test_url_contains_chart_host_and_encoded_config(self):
        dests = [
            DestinationStats(chat_id=1, name="Dest A", attempted=10, sent=10),
            DestinationStats(chat_id=2, name="Dest B", attempted=10, sent=9, failed=1),
        ]
        url = build_quickchart_url(dests)
        assert url.startswith("https://quickchart.io/chart?")
        assert "type" in url or "%22type%22" in url
        assert "800" in url


class TestScheduler:
    def test_before_hour_same_day(self):
        now = datetime(2026, 7, 14, 6, 0, 0, tzinfo=UTC_PLUS_3)
        secs = seconds_until_next_run(hour=8, tz_name="Etc/GMT-3", now=now)
        assert secs == pytest.approx(2 * 3600, abs=1)

    def test_after_hour_next_day(self):
        now = datetime(2026, 7, 14, 9, 0, 0, tzinfo=UTC_PLUS_3)
        secs = seconds_until_next_run(hour=8, tz_name="Etc/GMT-3", now=now)
        assert secs == pytest.approx(23 * 3600, abs=1)

    def test_exactly_on_hour_rolls_to_tomorrow(self):
        now = datetime(2026, 7, 14, 8, 0, 0, tzinfo=UTC_PLUS_3)
        secs = seconds_until_next_run(hour=8, tz_name="Etc/GMT-3", now=now)
        assert secs == pytest.approx(24 * 3600, abs=1)


def _sample_metrics(**overrides) -> DailyReportMetrics:
    base = dict(
        verdict="green",
        components=[
            ComponentStatus("pyrogram_session", "Pyrogram", True),
            ComponentStatus("sender_bot", "Sender", True),
            ComponentStatus("redis", "Redis", True),
            ComponentStatus("sqlite", "SQLite", True),
            ComponentStatus("n8n_webhook", "n8n", True),
            ComponentStatus("disk_space", "Disk", True),
        ],
        disk_free_gb=12.4,
        queues=QueueSnapshot(),
        received_24h=847,
        sent_24h=830,
        failed_24h=15,
        attempted_24h=845,
        success_rate_24h=98.2,
        destinations=[
            DestinationStats(1, "Dest A", 280, 280, 0, 2.1),
            DestinationStats(2, "Dest B", 280, 263, 3, 3.4),
            DestinationStats(3, "Dest C", 285, 248, 12, 5.8),
        ],
        volume_7d=[600, 750, 700, 850, 900, 880, 920],
        volume_7d_avg=812.0,
        report_date="Tue 14 Jul 2026",
        timezone_label="UTC+3",
    )
    base.update(overrides)
    return DailyReportMetrics(**base)


class TestNarrativeAndFormat:
    def test_green_narrative(self):
        text = build_narrative(_sample_metrics())
        assert "847" in text
        assert "98.2%" in text or "98.2" in text

    def test_red_narrative_mentions_component(self):
        m = _sample_metrics(
            verdict="red",
            components=[
                ComponentStatus("pyrogram_session", "Pyrogram", True),
                ComponentStatus("sender_bot", "Sender", False, "down"),
                ComponentStatus("redis", "Redis", True),
                ComponentStatus("sqlite", "SQLite", True),
                ComponentStatus("n8n_webhook", "n8n", True),
                ComponentStatus("disk_space", "Disk", True),
            ],
            queues=QueueSnapshot(pending=340),
            received_24h=340,
            success_rate_24h=0.0,
        )
        text = build_narrative(m)
        assert "🔴" in text
        assert "Sender" in text

    def test_html_contains_sections(self):
        html = format_report_html(_sample_metrics())
        assert "Daily Forward Health" in html
        assert "QUEUES NOW" in html
        assert "DESTINATIONS" in html
        assert "7d Volume" in html
        assert "<pre>" in html
