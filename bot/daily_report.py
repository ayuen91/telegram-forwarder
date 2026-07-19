"""
Daily health report formatting and Telegram delivery.

Sends:
  1. sendPhoto — QuickChart bar chart of per-destination success rates
  2. sendMessage — visual HTML digest with narrative summary
"""

from __future__ import annotations

import json
import logging
import urllib.parse
from typing import List, Optional

import aiohttp

from alerts import send_photo
from metrics import (
    DailyReportMetrics,
    DestinationStats,
    progress_bar,
    sparkline,
)

logger = logging.getLogger(__name__)

VERDICT_EMOJI = {"green": "🟢", "yellow": "🟡", "red": "🔴"}


def _truncate_name(name: str, max_len: int = 14) -> str:
    name = (name or "?").strip() or "?"
    # Strip HTML-sensitive chars so Telegram parse_mode=HTML stays valid
    for ch in ("<", ">", "&"):
        name = name.replace(ch, "")
    if len(name) <= max_len:
        return name
    return name[: max_len - 1] + "…"


def _latency_str(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    if seconds < 10:
        return f"{seconds:.1f}s"
    return f"{seconds:.0f}s"


def build_narrative(m: DailyReportMetrics) -> str:
    """Rule-based 2–3 sentence summary (no LLM)."""
    dest_count = len([d for d in m.destinations if d.attempted > 0]) or len(m.destinations)
    rate = m.success_rate_24h
    received = m.received_24h
    q = m.queues

    failed_components = [c.label for c in m.components if not c.passed]
    worst: Optional[DestinationStats] = None
    for d in m.destinations:
        if d.attempted == 0:
            continue
        if worst is None or d.success_rate < worst.success_rate:
            worst = d

    if m.verdict == "red":
        if failed_components:
            comps = ", ".join(failed_components)
            return (
                f"🔴 Critical issues detected ({comps}). "
                f"{received} messages received; {rate:.1f}% delivery across "
                f"{dest_count} channel(s). {q.pending} pending in queue."
            )
        return (
            f"🔴 Delivery degraded to {rate:.1f}% over the last 24h "
            f"({received} received, {m.failed_24h} failed). "
            f"Queue: pending {q.pending}, dead-letter {q.dead_letter}."
        )

    if m.verdict == "yellow":
        parts = []
        if q.dead_letter > 0:
            parts.append(
                f"⚠️ {q.dead_letter} message(s) stuck in dead-letter queue."
            )
        if q.retry > 0:
            parts.append(f"{q.retry} awaiting retry.")
        if worst and worst.failed > 0 and worst.success_rate < 95:
            parts.append(
                f"{_truncate_name(worst.name, 20)} had {worst.failed} failure(s) "
                f"({worst.success_rate:.0f}%)."
            )
        if failed_components:
            parts.append(f"Warnings: {', '.join(failed_components)}.")
        detail = " ".join(parts) if parts else "Some metrics need attention."
        return (
            f"{received} messages processed with {rate:.1f}% delivery. {detail}"
        )

    # green
    line = (
        f"Yesterday was smooth — {received} messages forwarded with "
        f"{rate:.1f}% delivery across {dest_count} channel(s). "
        f"All systems online"
    )
    if q.pending == 0 and q.retry == 0 and q.dead_letter == 0:
        line += ", queues empty."
    else:
        line += "."
    if worst and worst.attempted > 0 and worst.success_rate < 100 and worst.success_rate >= 95:
        line += (
            f" {_truncate_name(worst.name, 20)} dipped slightly "
            f"({worst.success_rate:.0f}%)."
        )
    return line


def build_quickchart_url(destinations: List[DestinationStats], w: int = 800, h: int = 400) -> str:
    """Build a QuickChart.io mixed dual-axis chart URL showing attempted volume and success rate."""
    active = [d for d in destinations if d.attempted > 0] or list(destinations)
    labels = [_truncate_name(d.name, 15) for d in active]
    rates = [round(d.success_rate, 1) for d in active]
    attempts = [d.attempted for d in active]

    config = {
        "type": "bar",
        "data": {
            "labels": labels,
            "datasets": [
                {
                    "type": "bar",
                    "label": "Volume (Messages)",
                    "data": attempts,
                    "backgroundColor": "rgba(54, 162, 235, 0.5)",
                    "borderColor": "rgba(54, 162, 235, 1)",
                    "borderWidth": 1,
                    "yAxisID": "yVolume",
                },
                {
                    "type": "line",
                    "label": "Success Rate (%)",
                    "data": rates,
                    "borderColor": "#10b981",
                    "backgroundColor": "#10b981",
                    "fill": False,
                    "yAxisID": "ySuccess",
                    "tension": 0.3,
                    "pointRadius": 4,
                }
            ],
        },
        "options": {
            "plugins": {
                "title": {
                    "display": True,
                    "text": "24h Volume & Delivery Success by Channel",
                    "color": "#e5e7eb",
                },
                "legend": {
                    "display": True,
                    "labels": {"color": "#e5e7eb"}
                },
            },
            "scales": {
                "yVolume": {
                    "type": "linear",
                    "position": "left",
                    "title": {
                        "display": True,
                        "text": "Messages Attempted",
                        "color": "#9ca3af",
                    },
                    "ticks": {"color": "#9ca3af"},
                    "grid": {"color": "#374151"},
                },
                "ySuccess": {
                    "type": "linear",
                    "position": "right",
                    "min": 0,
                    "max": 100,
                    "title": {
                        "display": True,
                        "text": "Success Rate (%)",
                        "color": "#9ca3af",
                    },
                    "ticks": {"color": "#9ca3af"},
                    "grid": {"drawOnChartArea": False},
                },
                "x": {
                    "ticks": {"color": "#9ca3af"},
                    "grid": {"display": False},
                },
            },
            "backgroundColor": "#1f2937",
        },
    }
    encoded = urllib.parse.quote(json.dumps(config, separators=(",", ":")))
    return f"https://quickchart.io/chart?w={w}&h={h}&bkg=%231f2937&c={encoded}"


def format_report_html(m: DailyReportMetrics) -> str:
    """Build the HTML digest (Message 2)."""
    emoji = VERDICT_EMOJI.get(m.verdict, "⚪")
    narrative = build_narrative(m)

    # Component status row — paired inline so emoji always aligns with its label.
    # Rendering two separate rows (labels + icons) breaks in Telegram because emoji
    # are double-width, causing the dots to drift under the wrong label.
    component_pairs = "  ".join(
        f"{'🟢' if c.passed else '🔴'} {c.label}"
        for c in m.components
    )
    disk_note = ""
    if m.disk_free_gb is not None:
        disk_note = f"  │  {m.disk_free_gb:.1f} GB free"

    bar_24h = progress_bar(m.success_rate_24h)
    spark = sparkline(m.volume_7d)
    avg = int(round(m.volume_7d_avg))

    # Destination lines (monospace pre block)
    dest_lines = []
    for d in m.destinations:
        if d.attempted == 0 and m.attempted_24h > 0:
            # Still show configured dests with no traffic lightly
            name = _truncate_name(d.name).ljust(14)
            dest_lines.append(f"{name}  {'░' * 10}   —   no traffic")
            continue
        name = _truncate_name(d.name).ljust(14)
        bar = progress_bar(d.success_rate)
        lat = _latency_str(d.avg_latency_s)
        fail = f"  ⚠️ {d.failed}" if d.failed else ""
        dest_lines.append(f"{name}  {bar}  ⚡ {lat}{fail}")

    if not dest_lines:
        dest_lines.append("(no destination activity in 24h)")

    dest_block = "\n".join(dest_lines)

    # Escape is not needed for our generated numbers; destination names from YAML
    # could contain < — sanitize names in dest_block already truncated from config.
    # Use HTML mode carefully: put bars in <pre>
    text = (
        f"📊 <b>Daily Forward Health</b> — {m.report_date} · 08:00 {m.timezone_label}\n\n"
        f"<blockquote>{narrative}</blockquote>\n"
        f"{emoji} <b>OVERALL HEALTH</b>\n\n"
        f"<code>{component_pairs}{disk_note}</code>\n\n"
        f"<b>QUEUES NOW</b>          <b>24H VOLUME</b>\n"
        f"<code>"
        f"Pending     {m.queues.pending:<5}  Received   {m.received_24h}\n"
        f"Retry       {m.queues.retry:<5}  Success    {bar_24h}\n"
        f"Dead-letter {m.queues.dead_letter:<5}  Failed     {m.failed_24h}"
        f"</code>\n\n"
        f"<b>DESTINATIONS (24h)</b>\n"
        f"<pre>{dest_block}</pre>\n"
        f"<b>7d Volume</b>   <code>{spark}</code>  (avg {avg}/day)"
    )
    return text


def format_chart_caption(m: DailyReportMetrics) -> str:
    emoji = VERDICT_EMOJI.get(m.verdict, "⚪")
    return (
        f"{emoji} Daily Forward Health — {m.report_date}\n"
        f"Delivery success by destination (last 24h)"
    )


async def send_daily_report(
    token: str,
    chat_id: int,
    metrics: DailyReportMetrics,
) -> None:
    """Deliver chart photo + HTML digest via the alert bot."""
    if not token or not chat_id:
        logger.warning("Alert bot not configured — skipping daily report")
        return

    chart_url = build_quickchart_url(metrics.destinations)
    caption = format_chart_caption(metrics)
    body = format_report_html(metrics)

    try:
        await send_photo(token, chat_id, chart_url, caption=caption)
    except Exception as e:
        logger.error(f"Daily report chart send failed: {e}")

    # Bypass alert cooldown — daily report is intentional once/day
    await _send_message_no_cooldown(token, chat_id, body)


async def _send_message_no_cooldown(token: str, chat_id: int, message: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error(f"Daily report message error {resp.status}: {body[:300]}")
                else:
                    logger.info("Daily health report sent")
    except Exception as e:
        logger.error(f"Failed to send daily report message: {e}")
