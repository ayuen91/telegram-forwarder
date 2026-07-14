"""Tests for TelegramBotSender helpers (no live API calls)."""

from unittest.mock import AsyncMock, patch

import pytest

from telegram_sender import TelegramBotSender


class TestTelegramBotSenderHelpers:
    def test_reply_params_none(self):
        assert TelegramBotSender._reply_params(None) is None

    def test_reply_params_message_id(self):
        assert TelegramBotSender._reply_params(42) == {"message_id": 42}

    def test_reply_params_string_id(self):
        assert TelegramBotSender._reply_params("99") == {"message_id": 99}

    def test_bot_api_entities_none(self):
        assert TelegramBotSender._bot_api_entities(None) is None
        assert TelegramBotSender._bot_api_entities([]) is None

    def test_bot_api_entities_text_link(self):
        entities = [{"type": "text_link", "offset": 0, "length": 4, "url": "https://x.com"}]
        result = TelegramBotSender._bot_api_entities(entities)
        assert result == [{"type": "text_link", "offset": 0, "length": 4, "url": "https://x.com"}]

    def test_bot_api_entities_text_mention(self):
        entities = [{"type": "text_mention", "offset": 0, "length": 5, "user_id": 12345}]
        result = TelegramBotSender._bot_api_entities(entities)
        assert result == [{"type": "text_mention", "offset": 0, "length": 5, "user": {"id": 12345}}]


class TestForwardToDestination:
    @pytest.fixture
    def sender(self):
        return TelegramBotSender(bot_token="test:token")

    @pytest.mark.asyncio
    async def test_text_uses_send_message_without_relay(self, sender):
        sender.send_message = AsyncMock(return_value=99)

        result = await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="text",
            payload={"message_id": 36, "text": "hello", "entities": []},
            processed_payload={"text_changed": False},
        )

        sender.send_message.assert_awaited_once_with(
            chat_id=-1001,
            text="hello",
            entities=[],
            reply_to_message_id=None,
        )
        assert result == {"sent_message_id": 99, "reply_mappings": [(36, 99)]}

    @pytest.mark.asyncio
    async def test_text_with_replacements_skips_entities(self, sender):
        sender.send_message = AsyncMock(return_value=100)

        await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="text",
            payload={"message_id": 36, "text": "hello", "entities": [{"type": "bold", "offset": 0, "length": 5}]},
            processed_payload={"text_changed": True, "processed_text": "world"},
        )

        sender.send_message.assert_awaited_once_with(
            chat_id=-1001,
            text="world",
            entities=None,
            reply_to_message_id=None,
        )

    @pytest.mark.asyncio
    async def test_media_requires_relay(self, sender):
        with pytest.raises(RuntimeError, match="requires userbot relay"):
            await sender.forward_to_destination(
                dest_chat_id=-1001,
                msg_type="photo",
                payload={"message_id": 36},
                processed_payload={},
            )

    @pytest.mark.asyncio
    async def test_media_uses_relay_copy(self, sender):
        sender.copy_message = AsyncMock(return_value=55)

        result = await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="photo",
            payload={"message_id": 36},
            processed_payload={},
            relay_chat_id=123456,
            relay_message_ids=[77],
        )

        sender.copy_message.assert_awaited_once_with(
            chat_id=-1001,
            from_chat_id=123456,
            message_id=77,
            caption=None,
            reply_to_message_id=None,
        )
        assert result["sent_message_id"] == 55
