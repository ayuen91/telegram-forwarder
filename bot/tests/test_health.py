"""Tests for health monitor startup gating and reactive failure rate alerts."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from health import HealthMonitor, HealthCheckResult


def _monitor(**kwargs) -> HealthMonitor:
    config = MagicMock()
    config.settings = MagicMock()
    config.settings.failure_rate_sample_size = 50
    config.settings.failure_rate_min_samples = 10
    config.settings.failure_rate_warning_threshold = 0.20
    config.settings.failure_rate_critical_threshold = 0.50
    config.settings.failure_rate_delta_threshold = 0.10
    return HealthMonitor(
        redis_client=MagicMock(),
        webhook_sender=MagicMock(),
        config=config,
        **kwargs,
    )


class TestStartupSelfTest:
    @pytest.mark.asyncio
    async def test_dead_letter_does_not_block_startup(self):
        monitor = _monitor()
        monitor.run_all_checks = AsyncMock(return_value=[
            HealthCheckResult(name="hydrogram_session", passed=True, level="critical"),
            HealthCheckResult(name="sender_bot", passed=True, level="critical"),
            HealthCheckResult(name="redis", passed=True, level="critical"),
            HealthCheckResult(name="sqlite", passed=True, level="critical"),
            HealthCheckResult(
                name="dead_letter_queue",
                passed=False,
                level="high",
                message="1 messages in dead letter queue",
            ),
        ])

        assert await monitor.startup_self_test() is True

    @pytest.mark.asyncio
    async def test_critical_failure_blocks_startup(self):
        monitor = _monitor()
        monitor.run_all_checks = AsyncMock(return_value=[
            HealthCheckResult(name="hydrogram_session", passed=False, level="critical", message="down"),
            HealthCheckResult(name="dead_letter_queue", passed=False, level="high", message="1 msg"),
        ])

        assert await monitor.startup_self_test() is False


class TestReactiveFailureRateAlerts:
    @pytest.fixture
    def monitor(self):
        m = _monitor(db_path=":memory:")
        m._send_telegram_alert = AsyncMock()
        return m

    @pytest.mark.asyncio
    async def test_warning_threshold_alert(self, monitor):
        """Crossing 20% warning threshold triggers instant alert."""
        # 12 failed out of 50 = 24% failure rate
        rows = [{"status": "failed"}] * 12 + [{"status": "sent"}] * 38

        mock_cursor = MagicMock()
        mock_cursor.fetchall = AsyncMock(return_value=rows)

        mock_db = MagicMock()
        mock_db.execute = AsyncMock(return_value=mock_cursor)

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_db)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        with patch("aiosqlite.connect", MagicMock(return_value=mock_cm)):
            result = await monitor._check_sustained_failure_rate()

        assert result.passed is False
        assert "12/50 recent deliveries failed (24.0%)" in result.message
        monitor._send_telegram_alert.assert_awaited_once()
        alert_text = monitor._send_telegram_alert.call_args[0][0]
        assert "Delivery Failure Rate Warning" in alert_text
        assert "24.0%" in alert_text

    @pytest.mark.asyncio
    async def test_delta_10_percent_increase_alert(self, monitor):
        """10% or greater change triggers a reactive update alert."""
        monitor._last_failure_rate = 0.20  # previously 20%
        monitor._last_failure_state = "warning"

        # Now 16 failed out of 50 = 32% failure rate (+12% delta increase)
        rows = [{"status": "failed"}] * 16 + [{"status": "sent"}] * 34

        mock_cursor = MagicMock()
        mock_cursor.fetchall = AsyncMock(return_value=rows)

        mock_db = MagicMock()
        mock_db.execute = AsyncMock(return_value=mock_cursor)

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_db)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        with patch("aiosqlite.connect", MagicMock(return_value=mock_cm)):
            result = await monitor._check_sustained_failure_rate()

        assert result.passed is False
        monitor._send_telegram_alert.assert_awaited_once()
        alert_text = monitor._send_telegram_alert.call_args[0][0]
        assert "Delivery Failure Rate Increased" in alert_text
        assert "20.0% ➔ 32.0%" in alert_text

    @pytest.mark.asyncio
    async def test_recovery_alert(self, monitor):
        """Dropping back below 20% triggers a recovery alert."""
        monitor._last_failure_rate = 0.30  # previously 30%
        monitor._last_failure_state = "warning"

        # Now 2 failed out of 50 = 4% failure rate
        rows = [{"status": "failed"}] * 2 + [{"status": "sent"}] * 48

        mock_cursor = MagicMock()
        mock_cursor.fetchall = AsyncMock(return_value=rows)

        mock_db = MagicMock()
        mock_db.execute = AsyncMock(return_value=mock_cursor)

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_db)
        mock_cm.__aexit__ = AsyncMock(return_value=None)

        with patch("aiosqlite.connect", MagicMock(return_value=mock_cm)):
            result = await monitor._check_sustained_failure_rate()

        assert result.passed is True
        monitor._send_telegram_alert.assert_awaited_once()
        alert_text = monitor._send_telegram_alert.call_args[0][0]
        assert "Delivery Failure Rate Recovered" in alert_text
        assert "4.0%" in alert_text
