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

    @pytest.mark.asyncio
    async def test_poll_uses_send_poll_without_relay(self, sender):
        sender.send_poll = AsyncMock(return_value=60)
        sender.copy_message = AsyncMock()

        poll_data = {
            "question": "Yes or no?",
            "options": ["Yes", "No"],
            "is_anonymous": True,
            "type": "regular",
        }
        result = await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="poll",
            payload={"message_id": 40, "poll": poll_data},
            processed_payload={},
        )

        sender.send_poll.assert_awaited_once_with(
            chat_id=-1001,
            poll=poll_data,
            reply_to_message_id=None,
        )
        sender.copy_message.assert_not_called()
        assert result == {"sent_message_id": 60, "reply_mappings": [(40, 60)]}

    @pytest.mark.asyncio
    async def test_location_uses_send_location_without_relay(self, sender):
        sender.send_location = AsyncMock(return_value=61)
        sender.copy_message = AsyncMock()

        location_data = {"latitude": 1.1, "longitude": 2.2}
        result = await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="location",
            payload={"message_id": 41, "location": location_data},
            processed_payload={},
        )

        sender.send_location.assert_awaited_once_with(
            chat_id=-1001,
            location=location_data,
            reply_to_message_id=None,
        )
        sender.copy_message.assert_not_called()
        assert result["sent_message_id"] == 61

    @pytest.mark.asyncio
    async def test_venue_uses_send_venue_without_relay(self, sender):
        sender.send_venue = AsyncMock(return_value=62)
        sender.copy_message = AsyncMock()

        venue_data = {
            "latitude": 3.3,
            "longitude": 4.4,
            "title": "Place",
            "address": "Street 1",
        }
        result = await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="venue",
            payload={"message_id": 42, "venue": venue_data},
            processed_payload={},
        )

        sender.send_venue.assert_awaited_once_with(
            chat_id=-1001,
            venue=venue_data,
            reply_to_message_id=None,
        )
        sender.copy_message.assert_not_called()
        assert result["sent_message_id"] == 62

    @pytest.mark.asyncio
    async def test_contact_uses_send_contact_without_relay(self, sender):
        sender.send_contact = AsyncMock(return_value=63)
        sender.copy_message = AsyncMock()

        contact_data = {
            "phone_number": "+123",
            "first_name": "Test",
            "last_name": "User",
        }
        result = await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="contact",
            payload={"message_id": 43, "contact": contact_data},
            processed_payload={},
        )

        sender.send_contact.assert_awaited_once_with(
            chat_id=-1001,
            contact=contact_data,
            reply_to_message_id=None,
        )
        sender.copy_message.assert_not_called()
        assert result["sent_message_id"] == 63

    @pytest.mark.asyncio
    async def test_quiz_poll_without_correct_option_falls_back_to_regular(self, sender):
        sender._call = AsyncMock(return_value={"message_id": 70})

        poll_data = {
            "question": "Quiz?",
            "options": ["A", "B"],
            "type": "quiz",
        }
        await sender.send_poll(
            chat_id=-1001,
            poll=poll_data,
        )

        sender._call.assert_awaited_once()
        payload = sender._call.await_args.args[1]
        assert payload["type"] == "regular"
        assert "correct_option_id" not in payload
