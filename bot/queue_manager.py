"""
Redis queue manager for message persistence and retry logic.

Queues:
  - queue:messages  — pending single messages
  - queue:albums    — pending album payloads
  - queue:failed    — failed deliveries awaiting retry
  - queue:dead_letter — permanently failed (after max retries)

Each message is stored as a JSON string. Failed messages include
retry_count and error info for debugging.
"""

import json
import logging
import time
from typing import Optional, Dict, Any

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


def _stable_json(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, default=str, separators=(",", ":"), sort_keys=True)


def _payload_identity(payload: Dict[str, Any]) -> tuple:
    """Stable identity for queue matching regardless of injected metadata fields."""
    chat_id = payload.get("chat_id")
    if payload.get("type") == "album" or payload.get("media_group_id"):
        return ("album", chat_id, str(payload.get("media_group_id")))
    return ("message", chat_id, payload.get("message_id"))


class QueueManager:
    """Manages Redis-backed message queues with retry logic."""

    QUEUE_MESSAGES = "queue:messages"
    QUEUE_ALBUMS = "queue:albums"
    QUEUE_FAILED = "queue:failed"
    QUEUE_DEAD_LETTER = "queue:dead_letter"

    # Backup key prefix (individual message data with TTL)
    BACKUP_PREFIX = "msg_backup"
    BACKUP_TTL = 86400  # 24 hours

    # Deferred retries (reply parent not ready) — short backoff, no max_retry burn
    QUEUE_DEFERRED = "queue:deferred"
    DEFER_MAX_ATTEMPTS = 15

    def __init__(self, redis_client: aioredis.Redis, max_retries: int = 3):
        self.redis = redis_client
        self.max_retries = max_retries

    async def enqueue(self, payload: Dict[str, Any], queue: str = None):
        """
        Push a message payload to the appropriate queue.
        Also stores a backup copy with TTL for recovery.
        """
        if queue is None:
            msg_type = payload.get("type", "")
            queue = self.QUEUE_ALBUMS if msg_type == "album" else self.QUEUE_MESSAGES

        payload_json = _stable_json(payload)
        message_id = payload.get("message_id", payload.get("media_group_id", "unknown"))

        # Push to queue
        await self.redis.rpush(queue, payload_json)

        # Store backup copy with TTL
        backup_key = f"{self.BACKUP_PREFIX}:{message_id}"
        await self.redis.set(backup_key, payload_json, ex=self.BACKUP_TTL)

        logger.debug(f"Enqueued message {message_id} to {queue}")

    async def enqueue_failed(self, payload: Dict[str, Any], error: str) -> str:
        """
        Push a failed message to the retry queue with error info.
        Returns 'dead_letter' if max retries exceeded, else 'failed'.
        """
        retry_count = payload.get("_retry_count", 0) + 1
        message_id = payload.get("message_id", "unknown")

        payload["_retry_count"] = retry_count
        payload["_last_error"] = error
        payload["_failed_at"] = time.time()

        if retry_count > self.max_retries:
            # Use default=str to guard against any non-JSON-serializable values
            # (e.g. Pyrogram enum types) that may have survived in the payload.
            payload_json = json.dumps(payload, default=str)
            await self.redis.rpush(self.QUEUE_DEAD_LETTER, payload_json)
            logger.error(
                f"Message {message_id} moved to dead letter queue "
                f"after {retry_count} retries: {error}"
            )
            return "dead_letter"

        payload_json = json.dumps(payload, default=str)
        await self.redis.rpush(self.QUEUE_FAILED, payload_json)
        logger.warning(
            f"Message {message_id} queued for retry "
            f"({retry_count}/{self.max_retries}): {error}"
        )
        return "failed"

    async def enqueue_deferred(self, payload: Dict[str, Any], error: str):
        """Re-queue for short retry when reply parent is not ready yet."""
        defer_count = payload.get("_defer_count", 0) + 1
        payload["_defer_count"] = defer_count
        payload["_last_error"] = error
        payload["_deferred_at"] = time.time()
        message_id = payload.get("message_id", payload.get("media_group_id", "unknown"))

        if defer_count > self.DEFER_MAX_ATTEMPTS:
            await self.enqueue_failed(payload, f"Reply parent not ready after {defer_count} attempts")
            return

        payload_json = _stable_json(payload)
        await self.redis.rpush(self.QUEUE_DEFERRED, payload_json)
        logger.info(
            f"Message {message_id} deferred ({defer_count}/{self.DEFER_MAX_ATTEMPTS}): {error}"
        )

    async def dequeue_deferred(self) -> Optional[Dict[str, Any]]:
        payload_json = await self.redis.lpop(self.QUEUE_DEFERRED)
        if payload_json:
            return json.loads(payload_json)
        return None

    async def dequeue_failed(self) -> Optional[Dict[str, Any]]:
        """Pop one message from the failed queue for retry."""
        payload_json = await self.redis.lpop(self.QUEUE_FAILED)
        if payload_json:
            return json.loads(payload_json)
        return None

    async def remove_from_queue(self, payload: Dict[str, Any], queue: str = None):
        """Remove a successfully processed message from the queue."""
        if queue is None:
            msg_type = payload.get("type", "")
            queue = self.QUEUE_ALBUMS if msg_type == "album" else self.QUEUE_MESSAGES

        target = _payload_identity(payload)
        items = await self.redis.lrange(queue, 0, -1)

        for item_json in items:
            try:
                item = json.loads(item_json)
            except json.JSONDecodeError:
                continue
            if _payload_identity(item) == target:
                await self.redis.lrem(queue, 1, item_json)
                break

        message_id = payload.get("message_id", payload.get("media_group_id", "unknown"))
        backup_key = f"{self.BACKUP_PREFIX}:{message_id}"
        await self.redis.delete(backup_key)

        logger.debug(f"Removed message {message_id} from {queue}")

    async def get_queue_depths(self) -> Dict[str, int]:
        """Get current depth of all queues. Used by health checks."""
        return {
            "messages": await self.redis.llen(self.QUEUE_MESSAGES),
            "albums": await self.redis.llen(self.QUEUE_ALBUMS),
            "failed": await self.redis.llen(self.QUEUE_FAILED),
            "deferred": await self.redis.llen(self.QUEUE_DEFERRED),
            "dead_letter": await self.redis.llen(self.QUEUE_DEAD_LETTER),
        }

    async def clear_dead_letter(self) -> int:
        """Remove all dead-letter entries (manual recovery). Returns count cleared."""
        count = await self.redis.llen(self.QUEUE_DEAD_LETTER)
        if count:
            await self.redis.delete(self.QUEUE_DEAD_LETTER)
            logger.warning(f"Cleared {count} message(s) from dead letter queue")
        return count
