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


class QueueManager:
    """Manages Redis-backed message queues with retry logic."""

    QUEUE_MESSAGES = "queue:messages"
    QUEUE_ALBUMS = "queue:albums"
    QUEUE_FAILED = "queue:failed"
    QUEUE_DEAD_LETTER = "queue:dead_letter"

    # Backup key prefix (individual message data with TTL)
    BACKUP_PREFIX = "msg_backup"
    BACKUP_TTL = 86400  # 24 hours

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

        payload_json = json.dumps(payload)
        message_id = payload.get("message_id", "unknown")

        # Push to queue
        await self.redis.rpush(queue, payload_json)

        # Store backup copy with TTL
        backup_key = f"{self.BACKUP_PREFIX}:{message_id}"
        await self.redis.set(backup_key, payload_json, ex=self.BACKUP_TTL)

        logger.debug(f"Enqueued message {message_id} to {queue}")

    async def enqueue_failed(self, payload: Dict[str, Any], error: str):
        """
        Push a failed message to the retry queue with error info.
        If max retries exceeded, moves to dead letter queue.
        """
        retry_count = payload.get("_retry_count", 0) + 1
        message_id = payload.get("message_id", "unknown")

        payload["_retry_count"] = retry_count
        payload["_last_error"] = error
        payload["_failed_at"] = time.time()

        if retry_count > self.max_retries:
            # Move to dead letter queue — requires manual intervention
            payload_json = json.dumps(payload)
            await self.redis.rpush(self.QUEUE_DEAD_LETTER, payload_json)
            logger.error(
                f"Message {message_id} moved to dead letter queue "
                f"after {retry_count} retries: {error}"
            )
        else:
            # Push to retry queue
            payload_json = json.dumps(payload)
            await self.redis.rpush(self.QUEUE_FAILED, payload_json)
            logger.warning(
                f"Message {message_id} queued for retry "
                f"({retry_count}/{self.max_retries}): {error}"
            )

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

        payload_json = json.dumps(payload)
        removed = await self.redis.lrem(queue, 1, payload_json)

        # Also clean up backup key
        message_id = payload.get("message_id", "unknown")
        backup_key = f"{self.BACKUP_PREFIX}:{message_id}"
        await self.redis.delete(backup_key)

        if removed:
            logger.debug(f"Removed message {message_id} from {queue}")

    async def get_queue_depths(self) -> Dict[str, int]:
        """Get current depth of all queues. Used by health checks."""
        return {
            "messages": await self.redis.llen(self.QUEUE_MESSAGES),
            "albums": await self.redis.llen(self.QUEUE_ALBUMS),
            "failed": await self.redis.llen(self.QUEUE_FAILED),
            "dead_letter": await self.redis.llen(self.QUEUE_DEAD_LETTER),
        }
