"""Tests for health monitor startup gating."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from health import HealthMonitor, HealthCheckResult


def _monitor(**kwargs) -> HealthMonitor:
    return HealthMonitor(
        redis_client=MagicMock(),
        webhook_sender=MagicMock(),
        config=MagicMock(),
        **kwargs,
    )


class TestStartupSelfTest:
    @pytest.mark.asyncio
    async def test_dead_letter_does_not_block_startup(self):
        monitor = _monitor()
        monitor.run_all_checks = AsyncMock(return_value=[
            HealthCheckResult(name="pyrogram_session", passed=True, level="critical"),
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
            HealthCheckResult(name="pyrogram_session", passed=False, level="critical", message="down"),
            HealthCheckResult(name="dead_letter_queue", passed=False, level="high", message="1 msg"),
        ])

        assert await monitor.startup_self_test() is False
