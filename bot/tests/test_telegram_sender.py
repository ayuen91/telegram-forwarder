"""Tests for TelegramBotSender helpers (no live API calls)."""

from unittest.mock import AsyncMock, patch
import pytest

from telegram_sender import TelegramBotSender, _normalize_html_for_bot_api


class TestNormalizeHtmlForBotApi:
    """_normalize_html_for_bot_api converts Pyrogram HTML to Bot API HTML."""

    def test_passthrough_plain_text(self):
        assert _normalize_html_for_bot_api("hello world") == "hello world"

    def test_passthrough_valid_bold(self):
        assert _normalize_html_for_bot_api("<b>bold</b>") == "<b>bold</b>"

    def test_passthrough_valid_italic(self):
        assert _normalize_html_for_bot_api("<i>italic</i>") == "<i>italic</i>"

    def test_passthrough_valid_underline(self):
        assert _normalize_html_for_bot_api("<u>under</u>") == "<u>under</u>"

    def test_passthrough_valid_strikethrough(self):
        assert _normalize_html_for_bot_api("<s>strike</s>") == "<s>strike</s>"

    def test_passthrough_valid_code(self):
        assert _normalize_html_for_bot_api("<code>x</code>") == "<code>x</code>"

    def test_passthrough_valid_blockquote(self):
        assert _normalize_html_for_bot_api("<blockquote>q</blockquote>") == "<blockquote>q</blockquote>"

    def test_passthrough_valid_link(self):
        assert _normalize_html_for_bot_api('<a href="https://t.me">t</a>') == '<a href="https://t.me">t</a>'

    # --- Spoiler ---

    def test_spoiler_converted(self):
        result = _normalize_html_for_bot_api("<spoiler>secret</spoiler>")
        assert result == "<tg-spoiler>secret</tg-spoiler>"

    def test_spoiler_nested_inside_bold(self):
        result = _normalize_html_for_bot_api("<b><spoiler>s</spoiler></b>")
        assert result == "<b><tg-spoiler>s</tg-spoiler></b>"

    def test_already_tg_spoiler_unchanged(self):
        result = _normalize_html_for_bot_api("<tg-spoiler>s</tg-spoiler>")
        assert result == "<tg-spoiler>s</tg-spoiler>"

    # --- Custom emoji ---

    def test_emoji_id_converted(self):
        result = _normalize_html_for_bot_api('<emoji id="5368324170671202286">👍</emoji>')
        assert result == '<tg-emoji emoji-id="5368324170671202286">👍</tg-emoji>'

    def test_emoji_id_with_whitespace_converted(self):
        result = _normalize_html_for_bot_api('<emoji  id="123">X</emoji>')
        assert result == '<tg-emoji emoji-id="123">X</tg-emoji>'

    def test_already_tg_emoji_unchanged(self):
        src = '<tg-emoji emoji-id="123">👍</tg-emoji>'
        assert _normalize_html_for_bot_api(src) == src

    # --- Mixed content ---

    def test_mixed_entities_all_converted(self):
        src = '<b>Bold</b> <spoiler>hidden</spoiler> <emoji id="99">🎉</emoji>'
        expected = '<b>Bold</b> <tg-spoiler>hidden</tg-spoiler> <tg-emoji emoji-id="99">🎉</tg-emoji>'
        assert _normalize_html_for_bot_api(src) == expected

    def test_multiple_spoilers(self):
        src = "<spoiler>a</spoiler> and <spoiler>b</spoiler>"
        expected = "<tg-spoiler>a</tg-spoiler> and <tg-spoiler>b</tg-spoiler>"
        assert _normalize_html_for_bot_api(src) == expected


class TestTelegramBotSenderHelpers:
    def test_reply_params_none(self):
        assert TelegramBotSender._reply_params(None) is None

    def test_reply_params_message_id(self):
        assert TelegramBotSender._reply_params(42) == {
            "message_id": 42,
            "allow_sending_without_reply": True,
        }

    def test_reply_params_string_id(self):
        assert TelegramBotSender._reply_params("99") == {
            "message_id": 99,
            "allow_sending_without_reply": True,
        }

    def test_reply_params_with_quote(self):
        res = TelegramBotSender._reply_params(42, quote="hello", quote_parse_mode="HTML", quote_position=5)
        assert res == {
            "message_id": 42,
            "allow_sending_without_reply": True,
            "quote": "hello",
            "quote_parse_mode": "HTML",
            "quote_position": 5,
        }


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
            payload={"message_id": 36, "text": "hello"},
            processed_payload={"text_changed": False},
        )

        sender.send_message.assert_awaited_once_with(
            chat_id=-1001,
            text="hello",
            parse_mode=None,
            reply_to_message_id=None,
        )
        assert result == {"sent_message_id": 99, "reply_mappings": [(36, 99)]}

    @pytest.mark.asyncio
    async def test_text_with_replacements_skips_entities(self, sender):
        sender.send_message = AsyncMock(return_value=100)

        await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="text",
            payload={"message_id": 36, "text": "hello"},
            processed_payload={"text_changed": True, "processed_text": "world"},
        )

        sender.send_message.assert_awaited_once_with(
            chat_id=-1001,
            text="world",
            parse_mode=None,
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
            processed_payload={"caption_changed": False},
            relay_chat_id=123456,
            relay_message_ids=[77],
        )

        sender.copy_message.assert_awaited_once_with(
            chat_id=-1001,
            from_chat_id=123456,
            message_id=77,
            caption="",      # empty string clears any spurious relay caption when source has no caption
            parse_mode=None,
            reply_to_message_id=None,
        )
        assert result["sent_message_id"] == 55

    @pytest.mark.asyncio
    async def test_animation_uses_native_copy_for_caption_and_emojis(self, sender):
        sender.copy_message = AsyncMock(return_value=88)

        result = await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="animation",
            payload={"message_id": 45, "caption": "GIF caption 👍", "caption_html": "GIF caption <tg-emoji emoji-id=\"123\">👍</tg-emoji>"},
            processed_payload={"caption_changed": False},
            relay_chat_id=123456,
            relay_message_ids=[99],
        )

        sender.copy_message.assert_awaited_once_with(
            chat_id=-1001,
            from_chat_id=123456,
            message_id=99,
            caption=None,
            parse_mode=None,
            reply_to_message_id=None,
        )
        assert result["sent_message_id"] == 88

    @pytest.mark.asyncio
    async def test_copy_message_custom_emoji_fallback(self, sender):
        call_count = 0

        async def mock_call(method, payload):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Bot API copyMessage failed: Bad Request: CANNOT_USE_CUSTOM_EMOJI")
            return {"message_id": 123}

        sender._call = mock_call

        result = await sender.copy_message(
            chat_id=-1001,
            from_chat_id=123456,
            message_id=77,
            caption="Hello <tg-emoji emoji-id=\"123\">😀</tg-emoji>",
            parse_mode="HTML",
        )
        assert result == 123
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_copy_message_animation_retry_backoff(self, sender):
        call_count = 0

        async def mock_call(method, payload):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("Bot API copyMessage failed: Bad Request: message to copy not found")
            return {"message_id": 456}

        sender._call = mock_call

        with patch("asyncio.sleep", AsyncMock()):
            result = await sender.copy_message(
                chat_id=-1001,
                from_chat_id=123456,
                message_id=88,
            )
        assert result == 456
        assert call_count == 3

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

    @pytest.mark.asyncio
    async def test_pin_chat_message(self, sender):
        sender._call = AsyncMock(return_value=True)

        res = await sender.pin_chat_message(
            chat_id=-1001,
            message_id=50,
            disable_notification=True,
        )

        assert res is True
        sender._call.assert_awaited_once_with(
            "pinChatMessage",
            {
                "chat_id": -1001,
                "message_id": 50,
                "disable_notification": True,
            },
        )

    @pytest.mark.asyncio
    async def test_forward_to_destination_passes_reply_markup(self, sender):
        sender.send_message = AsyncMock(return_value=123)
        reply_markup = {"inline_keyboard": [[{"text": "Click", "url": "https://test.com"}]]}

        result = await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="text",
            payload={
                "message_id": 36,
                "text": "hello",
                "reply_markup": reply_markup,
            },
            processed_payload={"text_changed": False},
        )

        sender.send_message.assert_awaited_once_with(
            chat_id=-1001,
            text="hello",
            parse_mode=None,
            reply_to_message_id=None,
            reply_markup=reply_markup,
        )
        assert result == {"sent_message_id": 123, "reply_mappings": [(36, 123)]}


class TestAlbumCaptionHtmlFix:
    """Album caption edits must preserve HTML formatting when the caption had entities."""

    @pytest.fixture
    def sender(self):
        return TelegramBotSender(bot_token="test:token")

    @pytest.mark.asyncio
    async def test_album_caption_plain_replacement_no_parse_mode(self, sender):
        """Plain-text caption replacement: edit_message_caption called without parse_mode."""
        sender.copy_messages = AsyncMock(return_value=[10])
        sender.edit_message_caption = AsyncMock()

        await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="album",
            payload={
                "message_id": 1,
                "items": [
                    {
                        "message_id": 1,
                        "caption": "old text",
                        "processed_caption": "new text",
                        # No processed_caption_html — plain-text caption
                    }
                ],
            },
            processed_payload={
                "items": [
                    {
                        "message_id": 1,
                        "caption": "old text",
                        "processed_caption": "new text",
                    }
                ]
            },
            relay_chat_id=999,
            relay_message_ids=[77],
        )

        sender.edit_message_caption.assert_awaited_once_with(
            -1001, 10, "new text", parse_mode=None
        )

    @pytest.mark.asyncio
    async def test_album_caption_html_replacement_uses_html_parse_mode(self, sender):
        """HTML caption replacement: edit_message_caption called WITH parse_mode='HTML'."""
        sender.copy_messages = AsyncMock(return_value=[20])
        sender.edit_message_caption = AsyncMock()

        await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="album",
            payload={
                "message_id": 2,
                "items": [
                    {
                        "message_id": 2,
                        "caption": "old text",
                        "caption_html": "<b>old text</b>",
                    }
                ],
            },
            processed_payload={
                "items": [
                    {
                        "message_id": 2,
                        "caption": "old text",
                        "caption_html": "<b>old text</b>",
                        "processed_caption": "new text",
                        "processed_caption_html": "<b>new text</b>",
                    }
                ]
            },
            relay_chat_id=999,
            relay_message_ids=[78],
        )

        sender.edit_message_caption.assert_awaited_once_with(
            -1001, 20, "<b>new text</b>", parse_mode="HTML"
        )

    @pytest.mark.asyncio
    async def test_album_caption_no_change_no_edit(self, sender):
        """No replacement: edit_message_caption must NOT be called."""
        sender.copy_messages = AsyncMock(return_value=[30])
        sender.edit_message_caption = AsyncMock()

        await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="album",
            payload={
                "message_id": 3,
                "items": [{"message_id": 3, "caption": "same", "caption_html": "<b>same</b>"}],
            },
            processed_payload={
                "items": [
                    {
                        "message_id": 3,
                        "caption": "same",
                        "caption_html": "<b>same</b>",
                        "processed_caption": "same",          # unchanged
                        "processed_caption_html": "<b>same</b>",
                    }
                ]
            },
            relay_chat_id=999,
            relay_message_ids=[79],
        )

        sender.edit_message_caption.assert_not_called()

    @pytest.mark.asyncio
    async def test_album_caption_html_spoiler_normalised(self, sender):
        """Spoiler tag in processed_caption_html is normalised before the edit."""
        sender.copy_messages = AsyncMock(return_value=[40])
        sender.edit_message_caption = AsyncMock()

        await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="album",
            payload={
                "message_id": 4,
                "items": [
                    {
                        "message_id": 4,
                        "caption": "old",
                        "caption_html": "<spoiler>old</spoiler>",
                    }
                ],
            },
            processed_payload={
                "items": [
                    {
                        "message_id": 4,
                        "caption": "old",
                        "caption_html": "<spoiler>old</spoiler>",
                        "processed_caption": "new",
                        "processed_caption_html": "<spoiler>new</spoiler>",
                    }
                ]
            },
            relay_chat_id=999,
            relay_message_ids=[80],
        )

        # <spoiler> must have been converted to <tg-spoiler> before the API call
        sender.edit_message_caption.assert_awaited_once_with(
            -1001, 40, "<tg-spoiler>new</tg-spoiler>", parse_mode="HTML"
        )


class TestTextHtmlNormalisationInForward:
    """text_html containing Pyrogram-style tags is normalised before sendMessage."""

    @pytest.fixture
    def sender(self):
        return TelegramBotSender(bot_token="test:token")

    @pytest.mark.asyncio
    async def test_text_html_with_spoiler_is_normalised(self, sender):
        """When text_html contains <spoiler> it is sent as <tg-spoiler> to the Bot API."""
        sender.send_message = AsyncMock(return_value=55)

        await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="text",
            payload={
                "message_id": 10,
                "text": "secret",
                "text_html": "<spoiler>secret</spoiler>",
            },
            processed_payload={"text_changed": False},
        )

        sender.send_message.assert_awaited_once_with(
            chat_id=-1001,
            text="<tg-spoiler>secret</tg-spoiler>",
            parse_mode="HTML",
            reply_to_message_id=None,
        )

    @pytest.mark.asyncio
    async def test_replaced_text_html_with_emoji_is_normalised(self, sender):
        """Processed text_html with Pyrogram <emoji> tag is normalised before send."""
        sender.send_message = AsyncMock(return_value=56)

        await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="text",
            payload={
                "message_id": 11,
                "text": "hi 👍",
                "text_html": 'hi <emoji id="99">👍</emoji>',
            },
            processed_payload={
                "text_changed": True,
                "processed_text": "hey 👍",
                "processed_text_html": 'hey <emoji id="99">👍</emoji>',
            },
        )

        sender.send_message.assert_awaited_once_with(
            chat_id=-1001,
            text='hey <tg-emoji emoji-id="99">👍</tg-emoji>',
            parse_mode="HTML",
            reply_to_message_id=None,
        )

