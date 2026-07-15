"""
Async daily scheduler for the forwarder health report.

Sleeps until the next configured local hour (default 08:00 UTC+3),
then builds and sends the report.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from daily_report import send_daily_report
from metrics import collect_report_metrics

logger = logging.getLogger(__name__)

# Fixed UTC+3. Prefer this over Etc/GMT-3 (POSIX sign is inverted).
UTC_PLUS_3 = timezone(timedelta(hours=3))


def resolve_tz(tz_name: str):
    """Resolve a timezone name; fall back to fixed UTC+3."""
    if not tz_name or tz_name in ("UTC+3", "Etc/GMT-3", "GMT+3"):
        return UTC_PLUS_3
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(tz_name)
    except Exception:
        logger.warning(f"Unknown timezone {tz_name!r} — using fixed UTC+3")
        return UTC_PLUS_3


def seconds_until_next_run(
    hour: int = 8,
    tz_name: str = "Etc/GMT-3",
    now: Optional[datetime] = None,
) -> float:
    """Seconds until the next occurrence of `hour:00` in the given timezone."""
    tz = resolve_tz(tz_name)

    if now is None:
        now = datetime.now(tz)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=tz)
    else:
        now = now.astimezone(tz)

    target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if now >= target:
        target = target + timedelta(days=1)
    return max(0.0, (target - now).total_seconds())


async def run_daily_report_once(
    health_monitor,
    queue_mgr,
    redis_client,
    config,
    db_path: str,
    alert_token: str,
    alert_chat_id: int,
    tz_name: str = "Etc/GMT-3",
) -> None:
    """Collect metrics and send one report."""
    results = await health_monitor.run_all_checks()
    destinations = config.get_active_destinations()
    metrics = await collect_report_metrics(
        health_results=results,
        queue_mgr=queue_mgr,
        redis_client=redis_client,
        db_path=db_path,
        destinations=destinations,
        tz_name=tz_name,
    )
    await send_daily_report(alert_token, alert_chat_id, metrics)
    logger.info(
        f"Daily report complete: verdict={metrics.verdict} "
        f"received={metrics.received_24h} success={metrics.success_rate_24h:.1f}%"
    )


def make_daily_report_factory(
    health_monitor,
    queue_mgr,
    redis_client,
    config,
    db_path: str,
    alert_token: str,
    alert_chat_id: int,
    hour: int = 8,
    tz_name: str = "Etc/GMT-3",
    run_on_start: bool = False,
    shutdown_event: Optional[asyncio.Event] = None,
):
    """Return an async coroutine suitable for supervised_task."""

    async def daily_report_loop():
        if run_on_start:
            logger.info("DAILY_REPORT_RUN_ON_START — sending report immediately")
            try:
                await run_daily_report_once(
                    health_monitor,
                    queue_mgr,
                    redis_client,
                    config,
                    db_path,
                    alert_token,
                    alert_chat_id,
                    tz_name=tz_name,
                )
            except Exception as e:
                logger.error(f"Daily report (on start) failed: {e}", exc_info=True)

        while True:
            if shutdown_event and shutdown_event.is_set():
                return

            wait_s = seconds_until_next_run(hour=hour, tz_name=tz_name)
            logger.info(
                f"Next daily report in {wait_s / 3600:.1f}h "
                f"(at {hour:02d}:00 {tz_name})"
            )

            remaining = wait_s
            while remaining > 0:
                if shutdown_event and shutdown_event.is_set():
                    return
                chunk = min(remaining, 60.0)
                await asyncio.sleep(chunk)
                remaining -= chunk

            if shutdown_event and shutdown_event.is_set():
                return

            try:
                await run_daily_report_once(
                    health_monitor,
                    queue_mgr,
                    redis_client,
                    config,
                    db_path,
                    alert_token,
                    alert_chat_id,
                    tz_name=tz_name,
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Daily report failed: {e}", exc_info=True)
                await asyncio.sleep(60)

    return daily_report_loop
