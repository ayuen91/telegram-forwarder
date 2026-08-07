"""Tests for album buffering logic."""

import json
import time
import pytest
from unittest.mock import AsyncMock


class TestAlbumBuffer:
    """Test album buffer logic with mocked Redis."""

    def setup_method(self):
        self.mock_redis = AsyncMock()
        self.mock_redis.exists.return_value = False

    @pytest.mark.asyncio
    async def test_first_item_sets_timer(self):
        """First album item should set the first_seen timer."""
        from album_buffer import AlbumBuffer

        self.mock_redis.hlen.return_value = 1
        buffer = AlbumBuffer(self.mock_redis, buffer_seconds=2.0)

        payload = {
            "message_id": 1,
            "chat_id": -100123,
            "media_group_id": "abc123",
            "type": "photo",
        }

        result = await buffer.add(payload)

        # Should not flush yet (only 1 item)
        assert result is None

        # Should set the hash entry
        self.mock_redis.hset.assert_called_once()
        # Should set the timer with NX
        self.mock_redis.set.assert_called_once()
        call_kwargs = self.mock_redis.set.call_args
        assert call_kwargs[1]["nx"] is True

    @pytest.mark.asyncio
    async def test_flush_at_max_size(self):
        """Album should flush immediately when reaching 10 items."""
        from album_buffer import AlbumBuffer

        self.mock_redis.hlen.return_value = 10  # Max album size

        # Mock hgetall to return 10 items
        items = {}
        for i in range(10):
            items[str(i)] = json.dumps({
                "message_id": i,
                "chat_id": -100123,
                "media_group_id": "full_album",
                "type": "photo",
            })
        self.mock_redis.hgetall.return_value = items

        buffer = AlbumBuffer(self.mock_redis, buffer_seconds=2.0)

        payload = {
            "message_id": 10,
            "chat_id": -100123,
            "media_group_id": "full_album",
            "type": "photo",
        }

        result = await buffer.add(payload)

        # Should flush immediately (returned album payload)
        assert result is not None
        assert result["type"] == "album"
        assert result["media_group_id"] == "full_album"
        assert result["item_count"] == 10

    @pytest.mark.asyncio
    async def test_no_media_group_id_skips(self):
        """Message without media_group_id should be skipped."""
        from album_buffer import AlbumBuffer

        buffer = AlbumBuffer(self.mock_redis, buffer_seconds=2.0)

        payload = {"message_id": 1, "chat_id": -100123}  # No media_group_id

        result = await buffer.add(payload)
        assert result is None
        self.mock_redis.hset.assert_not_called()

    @pytest.mark.asyncio
    async def test_flush_expired_album(self):
        """Albums older than buffer_seconds should be flushed."""
        from album_buffer import AlbumBuffer

        # Mock scan to return one timer key
        self.mock_redis.scan.return_value = (
            0,  # cursor (0 = done)
            [b"album:xyz:first_seen"],
        )

        # Timer set 3 seconds ago (buffer is 2s)
        self.mock_redis.get.return_value = str(time.time() - 3.0)

        # Mock album items
        self.mock_redis.hgetall.return_value = {
            "1": json.dumps({"message_id": 1, "chat_id": -100, "type": "photo"}),
            "2": json.dumps({"message_id": 2, "chat_id": -100, "type": "photo"}),
        }

        buffer = AlbumBuffer(self.mock_redis, buffer_seconds=2.0)
        completed = await buffer.check_and_flush_expired()

        assert len(completed) == 1
        assert completed[0]["type"] == "album"
        assert completed[0]["item_count"] == 2

        # Verify cleanup
        self.mock_redis.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_not_expired_album_stays(self):
        """Albums within the buffer window should not be flushed."""
        from album_buffer import AlbumBuffer

        self.mock_redis.scan.return_value = (
            0,
            [b"album:xyz:first_seen"],
        )

        # Timer set 0.5 seconds ago (buffer is 2s — not expired)
        self.mock_redis.get.return_value = str(time.time() - 0.5)

        buffer = AlbumBuffer(self.mock_redis, buffer_seconds=2.0)
        completed = await buffer.check_and_flush_expired()

        assert len(completed) == 0
        self.mock_redis.hgetall.assert_not_called()

    @pytest.mark.asyncio
    async def test_items_sorted_by_message_id(self):
        """Flushed album items should be sorted by message_id."""
        from album_buffer import AlbumBuffer

        self.mock_redis.hgetall.return_value = {
            "30": json.dumps({"message_id": 30, "chat_id": -100, "type": "photo"}),
            "10": json.dumps({"message_id": 10, "chat_id": -100, "type": "photo"}),
            "20": json.dumps({"message_id": 20, "chat_id": -100, "type": "photo"}),
        }

        buffer = AlbumBuffer(self.mock_redis, buffer_seconds=2.0)
        result = await buffer._flush_album("test_group")

        assert result is not None
        ids = [item["message_id"] for item in result["items"]]
        assert ids == [10, 20, 30]

    @pytest.mark.asyncio
    async def test_reply_to_message_id_resolution(self):
        """Album flush should resolve reply_to_message_id across all items and cast group_id to str."""
        from album_buffer import AlbumBuffer

        self.mock_redis.hgetall.return_value = {
            "10": json.dumps({"message_id": 10, "chat_id": -100, "type": "photo", "reply_to_message_id": None, "media_group_id": 14279032192983933}),
            "20": json.dumps({"message_id": 20, "chat_id": -100, "type": "photo", "reply_to_message_id": 29960, "media_group_id": 14279032192983933}),
        }

        buffer = AlbumBuffer(self.mock_redis, buffer_seconds=2.0)
        result = await buffer._flush_album(14279032192983933)

        assert result is not None
        assert result["media_group_id"] == "14279032192983933"
        assert result["reply_to_message_id"] == 29960
        assert result["items"][0]["reply_to_message_id"] == 29960
        assert result["items"][0]["media_group_id"] == "14279032192983933"
        assert result["items"][1]["reply_to_message_id"] == 29960
        assert result["items"][1]["media_group_id"] == "14279032192983933"

    @pytest.mark.asyncio
    async def test_late_item_forwarded_individually(self):
        """Late album stragglers should be forwarded, not dropped."""
        from album_buffer import AlbumBuffer

        self.mock_redis.exists.return_value = True
        buffer = AlbumBuffer(self.mock_redis, buffer_seconds=2.0)

        payload = {
            "message_id": 99,
            "chat_id": -100123,
            "media_group_id": "late_group",
            "type": "photo",
        }

        result = await buffer.add(payload)

        assert result is not None
        assert result.get("_late_album_item") is True
        assert "media_group_id" not in result
        assert result["message_id"] == 99
        self.mock_redis.hset.assert_not_called()

