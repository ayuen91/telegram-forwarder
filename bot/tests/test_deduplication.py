"""Tests for message deduplication logic."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock


class TestDeduplication:
    """Test dedup logic without requiring a real Redis connection."""

    def setup_method(self):
        """Create a mock Redis client for each test."""
        self.mock_redis = AsyncMock()

    @pytest.mark.asyncio
    async def test_new_message_returns_true(self):
        """First time seeing a message should return True (new)."""
        from deduplication import Deduplication

        self.mock_redis.set.return_value = True  # SET NX returns True when key was set
        dedup = Deduplication(self.mock_redis)

        result = await dedup.is_new(chat_id=-100123, message_id=456)
        assert result is True

        # Verify SET was called with correct key and params
        self.mock_redis.set.assert_called_once_with(
            "dedup:-100123:456", "1", nx=True, ex=86400
        )

    @pytest.mark.asyncio
    async def test_duplicate_message_returns_false(self):
        """Seeing the same message again should return False (duplicate)."""
        from deduplication import Deduplication

        self.mock_redis.set.return_value = None  # SET NX returns None when key exists
        dedup = Deduplication(self.mock_redis)

        result = await dedup.is_new(chat_id=-100123, message_id=456)
        assert result is False

    @pytest.mark.asyncio
    async def test_different_messages_are_independent(self):
        """Different message IDs should be tracked independently."""
        from deduplication import Deduplication

        self.mock_redis.set.return_value = True
        dedup = Deduplication(self.mock_redis)

        result1 = await dedup.is_new(chat_id=-100123, message_id=1)
        result2 = await dedup.is_new(chat_id=-100123, message_id=2)

        assert result1 is True
        assert result2 is True
        assert self.mock_redis.set.call_count == 2

    @pytest.mark.asyncio
    async def test_different_chats_are_independent(self):
        """Same message ID in different chats should be tracked separately."""
        from deduplication import Deduplication

        self.mock_redis.set.return_value = True
        dedup = Deduplication(self.mock_redis)

        await dedup.is_new(chat_id=-100111, message_id=1)
        await dedup.is_new(chat_id=-100222, message_id=1)

        # Should use different keys
        calls = self.mock_redis.set.call_args_list
        assert calls[0][0][0] == "dedup:-100111:1"
        assert calls[1][0][0] == "dedup:-100222:1"

    @pytest.mark.asyncio
    async def test_ttl_is_24_hours(self):
        """Dedup keys should expire after 24 hours (86400 seconds)."""
        from deduplication import Deduplication

        self.mock_redis.set.return_value = True
        dedup = Deduplication(self.mock_redis)

        await dedup.is_new(chat_id=-100123, message_id=1)

        # Verify TTL parameter
        call_kwargs = self.mock_redis.set.call_args
        assert call_kwargs[1]["ex"] == 86400

    @pytest.mark.asyncio
    async def test_mark_inflight(self):
        from deduplication import Deduplication

        dedup = Deduplication(self.mock_redis)
        await dedup.mark_inflight(chat_id=-100123, message_ids=[10, 11])

        self.mock_redis.sadd.assert_called_once_with("inflight:-100123", "10", "11")
        self.mock_redis.expire.assert_called_once_with("inflight:-100123", 600)

    @pytest.mark.asyncio
    async def test_clear_inflight(self):
        from deduplication import Deduplication

        dedup = Deduplication(self.mock_redis)
        await dedup.clear_inflight(chat_id=-100123, message_ids=[10])

        self.mock_redis.srem.assert_called_once_with("inflight:-100123", "10")

    @pytest.mark.asyncio
    async def test_is_inflight(self):
        from deduplication import Deduplication

        self.mock_redis.sismember.return_value = True
        dedup = Deduplication(self.mock_redis)

        assert await dedup.is_inflight(chat_id=-100123, message_id=10) is True
        self.mock_redis.sismember.assert_called_once_with("inflight:-100123", "10")
