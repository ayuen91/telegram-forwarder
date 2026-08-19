"""
Message deduplication via Redis SETEX.

Uses Redis SET with NX (set-if-not-exists) + 24h expiry to detect
duplicate messages. Handles Pyrogram reconnection replays where
the last N messages may be re-delivered.
"""

import logging

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class Deduplication:
    """Check if a message has already been processed."""

    # Key prefix for dedup entries
    KEY_PREFIX = "dedup"
    INFLIGHT_PREFIX = "inflight"
    TOMBSTONE_PREFIX = "tombstone"

    # How long to remember a message_id (24 hours)
    TTL_SECONDS = 86400
    INFLIGHT_TTL = 600  # 10 minutes — covers worker delay + relay + Bot API send
    TOMBSTONE_TTL = 600  # 10 minutes — covers queued message lifespan

    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

    def _inflight_key(self, chat_id: int) -> str:
        return f"{self.INFLIGHT_PREFIX}:{chat_id}"

    def _tombstone_key(self, chat_id: int, message_id: int) -> str:
        return f"{self.TOMBSTONE_PREFIX}:{chat_id}:{message_id}"

    async def is_new(self, chat_id: int, message_id: int) -> bool:
        """
        Check if message is new (not seen before).

        Returns True if the message is new and should be processed.
        Returns False if it's a duplicate (already seen within TTL).

        Uses SET NX (set-if-not-exists) — atomic check-and-set in one call.
        """
        key = f"{self.KEY_PREFIX}:{chat_id}:{message_id}"

        # SET with NX returns True if the key was set (new), None if it existed (duplicate)
        result = await self.redis.set(key, "1", nx=True, ex=self.TTL_SECONDS)

        if result:
            logger.debug(f"Dedup: new message {message_id} in chat {chat_id}")
            return True
        else:
            logger.debug(f"Dedup: duplicate message {message_id} in chat {chat_id}, skipping")
            return False

    async def mark_inflight(self, chat_id: int, message_ids: list):
        """Track message ids currently being forwarded (for reply ordering)."""
        if not message_ids:
            return
        key = self._inflight_key(chat_id)
        await self.redis.sadd(key, *[str(mid) for mid in message_ids])
        await self.redis.expire(key, self.INFLIGHT_TTL)

    async def clear_inflight(self, chat_id: int, message_ids: list):
        """Remove message ids from the in-flight set after forwarding completes."""
        if not message_ids:
            return
        key = self._inflight_key(chat_id)
        await self.redis.srem(key, *[str(mid) for mid in message_ids])

    async def is_inflight(self, chat_id: int, message_id: int) -> bool:
        """True if the parent message is still being processed."""
        key = self._inflight_key(chat_id)
        return bool(await self.redis.sismember(key, str(message_id)))

    async def mark_deleted(self, chat_id: int, message_ids: list):
        """Set tombstone flags for messages deleted at source to abort in-flight sends."""
        if not message_ids:
            return
        for mid in message_ids:
            try:
                key = self._tombstone_key(chat_id, int(mid))
                await self.redis.set(key, "1", ex=self.TOMBSTONE_TTL)
            except Exception as e:
                logger.debug(f"Tombstone write skipped for msg {mid}: {e}")

    async def is_deleted(self, chat_id: int, message_id: int) -> bool:
        """True if the message was deleted at source before forwarding started."""
        try:
            key = self._tombstone_key(chat_id, int(message_id))
            return bool(await self.redis.exists(key))
        except Exception as e:
            logger.debug(f"Tombstone check skipped for msg {message_id}: {e}")
            return False

