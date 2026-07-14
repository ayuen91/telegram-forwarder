"""Telegram alert delivery for operational failures."""

import logging
import time
from typing import Dict

import aiohttp

logger = logging.getLogger(__name__)

_last_alert: Dict[str, float] = {}
COOLDOWN_SECONDS = 300  # 5 minutes per alert key


async def send_alert(
    token: str,
    chat_id: int,
    message: str,
    alert_key: str = "",
    cooldown: int = COOLDOWN_SECONDS,
) -> None:
    if not token or not chat_id:
        return

    key = alert_key or message[:80]
    now = time.time()
    if now - _last_alert.get(key, 0) < cooldown:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    _last_alert[key] = now
                    logger.info(f"Alert sent: {alert_key or 'operational'}")
                else:
                    body = await resp.text()
                    logger.error(f"Alert API error {resp.status}: {body[:200]}")
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")
