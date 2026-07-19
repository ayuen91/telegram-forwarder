"""Telegram alert delivery for operational failures and daily reports."""

import logging
import time
from typing import Dict, Optional

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


async def send_photo(
    token: str,
    chat_id: int,
    photo_url: str,
    caption: Optional[str] = None,
) -> None:
    """Send a photo (e.g. QuickChart URL) via the alert bot. No cooldown.

    Telegram's sendPhoto rejects URLs that are too long or that its servers
    cannot fetch (e.g. QuickChart URLs with a large embedded JSON config).
    To avoid the 400 "failed to get HTTP URL content" error we download the
    image ourselves and upload the raw bytes as multipart/form-data instead.
    Falls back to the plain URL method if the download step fails.
    """
    if not token or not chat_id or not photo_url:
        return

    api_url = f"https://api.telegram.org/bot{token}/sendPhoto"

    async with aiohttp.ClientSession() as session:
        # ── Step 1: download the image ────────────────────────────────
        image_bytes: Optional[bytes] = None
        try:
            async with session.get(
                photo_url, timeout=aiohttp.ClientTimeout(total=20)
            ) as dl:
                if dl.status == 200:
                    image_bytes = await dl.read()
                else:
                    logger.warning(
                        f"Chart download returned {dl.status} — "
                        "falling back to URL upload"
                    )
        except Exception as dl_err:
            logger.warning(
                f"Chart download failed ({dl_err}) — falling back to URL upload"
            )

        # ── Step 2: upload (binary preferred, URL fallback) ───────────
        try:
            if image_bytes:
                # Binary multipart upload — always accepted by Telegram
                form = aiohttp.FormData()
                form.add_field("chat_id", str(chat_id))
                form.add_field(
                    "photo",
                    image_bytes,
                    filename="chart.png",
                    content_type="image/png",
                )
                if caption:
                    form.add_field("caption", caption[:1024])
                    form.add_field("parse_mode", "HTML")

                async with session.post(
                    api_url, data=form, timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        logger.info("Alert photo sent (binary upload)")
                    else:
                        body = await resp.text()
                        logger.error(
                            f"Alert sendPhoto error {resp.status}: {body[:200]}"
                        )
                        raise RuntimeError(f"sendPhoto failed: {resp.status}")
            else:
                # URL fallback — may fail for very long QuickChart URLs
                payload: dict = {"chat_id": chat_id, "photo": photo_url}
                if caption:
                    payload["caption"] = caption[:1024]
                    payload["parse_mode"] = "HTML"

                async with session.post(
                    api_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    if resp.status == 200:
                        logger.info("Alert photo sent (URL upload)")
                    else:
                        body = await resp.text()
                        logger.error(
                            f"Alert sendPhoto error {resp.status}: {body[:200]}"
                        )
                        raise RuntimeError(f"sendPhoto failed: {resp.status}")
        except RuntimeError:
            raise
        except Exception as e:
            logger.error(f"Failed to send alert photo: {e}")
            raise
