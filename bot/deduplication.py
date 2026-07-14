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

    # How long to remember a message_id (24 hours)
    TTL_SECONDS = 86400

    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

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
