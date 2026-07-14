"""
Album buffer — collects media group items in Redis before forwarding.

When Telegram sends an album, each item arrives as a separate update
sharing the same media_group_id. This buffer collects them over a
2-second window, then bundles and fires a single webhook to n8n.

Redis data structures:
  - album:{group_id}  — HASH: message_id -> payload JSON
  - album:{group_id}:first_seen — STRING: timestamp of first item

The flush worker (run by main.py as a supervised task) polls every
500ms for expired album timers.
"""

import json
import logging
import time
from typing import Dict, Any, List, Optional

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class AlbumBuffer:
    """Collect album items in Redis, flush when collection window expires."""

    ALBUM_PREFIX = "album"
    MAX_ALBUM_SIZE = 10  # Telegram's maximum album size

    def __init__(
        self,
        redis_client: aioredis.Redis,
        buffer_seconds: float = 2.0,
    ):
        self.redis = redis_client
        self.buffer_seconds = buffer_seconds

    async def add(self, payload: Dict[str, Any]):
        """
        Add a message to its album buffer.

        If this is the first item in the album, sets the timer.
        If the album reaches MAX_ALBUM_SIZE, returns it immediately for flushing.

        Returns:
            List of payloads if album is full (10 items), None otherwise.
        """
        group_id = payload.get("media_group_id")
        message_id = payload.get("message_id")

        if not group_id:
            logger.warning(f"Message {message_id} has no media_group_id, skipping album buffer")
            return None

        album_key = f"{self.ALBUM_PREFIX}:{group_id}"
        timer_key = f"{self.ALBUM_PREFIX}:{group_id}:first_seen"

        # Store this item in the album hash
        payload_json = json.dumps(payload, default=str)
        await self.redis.hset(album_key, str(message_id), payload_json)

        # Set first_seen timestamp if not already set (SETNX = set-if-not-exists)
        await self.redis.set(timer_key, str(time.time()), nx=True, ex=30)
        # 30s TTL on timer key is a safety net — albums should flush within 2s

        # Check if album is full (10 items = Telegram max)
        album_size = await self.redis.hlen(album_key)
        if album_size >= self.MAX_ALBUM_SIZE:
            logger.info(f"Album {group_id} reached max size ({album_size}), flushing immediately")
            return await self._flush_album(group_id)

        logger.debug(f"Buffered item {message_id} for album {group_id} ({album_size} items)")
        return None

    async def check_and_flush_expired(self) -> List[Dict[str, Any]]:
        """
        Check for albums whose collection window has expired.
        Called by the flush worker every 500ms.

        Returns list of completed album payloads (each is a list of items).
        """
        completed_albums = []
        now = time.time()

        # Scan for all album timer keys
        cursor = 0
        while True:
            cursor, keys = await self.redis.scan(
                cursor=cursor,
                match=f"{self.ALBUM_PREFIX}:*:first_seen",
                count=50,
            )

            for timer_key in keys:
                try:
                    timer_key_str = timer_key.decode() if isinstance(timer_key, bytes) else timer_key
                    first_seen_raw = await self.redis.get(timer_key)

                    if first_seen_raw is None:
                        continue

                    first_seen = float(first_seen_raw)
                    elapsed = now - first_seen

                    if elapsed >= self.buffer_seconds:
                        # Extract group_id from key: "album:{group_id}:first_seen"
                        group_id = timer_key_str.replace(f"{self.ALBUM_PREFIX}:", "").replace(
                            ":first_seen", ""
                        )
                        album_items = await self._flush_album(group_id)
                        if album_items:
                            completed_albums.append(album_items)

                except Exception as e:
                    logger.error(f"Error checking album timer {timer_key}: {e}")

            if cursor == 0:
                break

        return completed_albums

    async def _flush_album(self, group_id: str) -> Optional[Dict[str, Any]]:
        """
        Collect all items from an album buffer and clean up Redis keys.

        Returns a bundled album payload dict, or None if empty.
        """
        album_key = f"{self.ALBUM_PREFIX}:{group_id}"
        timer_key = f"{self.ALBUM_PREFIX}:{group_id}:first_seen"

        try:
            # Get all items
            raw_items = await self.redis.hgetall(album_key)
            if not raw_items:
                # Clean up timer key if album is empty
                await self.redis.delete(timer_key)
                return None

            # Parse items and sort by message_id to preserve order
            items = []
            for msg_id, payload_json in raw_items.items():
                payload = json.loads(payload_json)
                items.append(payload)

            items.sort(key=lambda x: x.get("message_id", 0))

            # Build bundled album payload
            album_payload = {
                "type": "album",
                "media_group_id": group_id,
                "chat_id": items[0].get("chat_id"),
                "item_count": len(items),
                "items": items,
                "timestamp": items[0].get("timestamp"),
            }

            # Clean up Redis keys
            await self.redis.delete(album_key, timer_key)

            logger.info(
                f"Flushed album {group_id}: {len(items)} items "
                f"from chat {album_payload['chat_id']}"
            )
            return album_payload

        except Exception as e:
            logger.error(f"Error flushing album {group_id}: {e}", exc_info=True)
            return None

    async def flush_stale_albums(self):
        """
        Flush any albums that were left over from a previous bot session.
        Called once at startup to recover from mid-album crashes.
        """
        cursor = 0
        flushed = 0

        while True:
            cursor, keys = await self.redis.scan(
                cursor=cursor,
                match=f"{self.ALBUM_PREFIX}:*:first_seen",
                count=50,
            )

            for timer_key in keys:
                try:
                    timer_key_str = timer_key.decode() if isinstance(timer_key, bytes) else timer_key
                    group_id = timer_key_str.replace(f"{self.ALBUM_PREFIX}:", "").replace(
                        ":first_seen", ""
                    )
                    result = await self._flush_album(group_id)
                    if result:
                        flushed += 1
                except Exception as e:
                    logger.error(f"Error flushing stale album: {e}")

            if cursor == 0:
                break

        if flushed:
            logger.info(f"Flushed {flushed} stale albums from previous session")

        return flushed
