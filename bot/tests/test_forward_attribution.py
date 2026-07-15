"""Tests for forward attribution helpers and native-forward delivery path."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from forward_attribution import (
    serialize_forward_origin,
    extract_channel_origin,
    ForwardAttributionChecker,
    attach_native_forward_flag,
)
from telegram_sender import TelegramBotSender


class TestSerializeForwardOrigin:
    def test_legacy_channel_forward(self):
        msg = SimpleNamespace(
            forward_origin=None,
            forward_from_chat=SimpleNamespace(id=-100111, type=SimpleNamespace(value="channel")),
            forward_from_message_id=42,
            forward_date=None,
            forward_signature="Author",
        )
        result = serialize_forward_origin(msg)
        assert result == {
            "type": "channel",
            "chat_id": -100111,
            "message_id": 42,
            "author_signature": "Author",
        }

    def test_modern_origin_channel(self):
        origin = SimpleNamespace(
            type=SimpleNamespace(value="channel"),
            chat=SimpleNamespace(id=-100222),
            message_id=99,
            date=None,
            author_signature=None,
        )
        msg = SimpleNamespace(forward_origin=origin)
        result = serialize_forward_origin(msg)
        assert result["type"] == "channel"
        assert result["chat_id"] == -100222
        assert result["message_id"] == 99

    def test_no_forward(self):
        msg = SimpleNamespace(
            forward_origin=None,
            forward_from_chat=None,
            forward_from_message_id=None,
        )
        assert serialize_forward_origin(msg) is None

    def test_user_origin_ignored(self):
        origin = SimpleNamespace(
            type=SimpleNamespace(value="user"),
            chat=None,
            message_id=None,
            sender_user=SimpleNamespace(id=1),
        )
        msg = SimpleNamespace(forward_origin=origin)
        assert serialize_forward_origin(msg) is None


class TestExtractChannelOrigin:
    def test_single_message(self):
        payload = {
            "type": "text",
            "forward_origin": {"type": "channel", "chat_id": -1001, "message_id": 5},
        }
        origin = extract_channel_origin(payload)
        assert origin["chat_id"] == -1001
        assert origin["message_ids"] == [5]

    def test_album_same_origin(self):
        payload = {
            "type": "album",
            "items": [
                {"message_id": 1, "forward_origin": {"type": "channel", "chat_id": -1001, "message_id": 10}},
                {"message_id": 2, "forward_origin": {"type": "channel", "chat_id": -1001, "message_id": 11}},
            ],
        }
        origin = extract_channel_origin(payload)
        assert origin["message_ids"] == [10, 11]

    def test_album_mixed_origins(self):
        payload = {
            "type": "album",
            "items": [
                {"message_id": 1, "forward_origin": {"type": "channel", "chat_id": -1001, "message_id": 10}},
                {"message_id": 2, "forward_origin": {"type": "channel", "chat_id": -1002, "message_id": 11}},
            ],
        }
        assert extract_channel_origin(payload) is None

    def test_album_partial_forward(self):
        payload = {
            "type": "album",
            "items": [
                {"message_id": 1, "forward_origin": {"type": "channel", "chat_id": -1001, "message_id": 10}},
                {"message_id": 2},
            ],
        }
        assert extract_channel_origin(payload) is None


class TestForwardAttributionChecker:
    def _config(self, enabled=True, allowlist=None):
        fa = SimpleNamespace(
            enabled=enabled,
            allowed_origin_channels=allowlist or [],
        )
        return SimpleNamespace(settings=SimpleNamespace(forward_attribution=fa))

    @pytest.mark.asyncio
    async def test_disabled(self):
        sender = MagicMock()
        checker = ForwardAttributionChecker(sender)
        payload = {
            "type": "text",
            "forward_origin": {"type": "channel", "chat_id": -1001, "message_id": 1},
        }
        assert await checker.should_use_native_forward(payload, self._config(enabled=False)) is False

    @pytest.mark.asyncio
    async def test_allowlist_miss(self):
        sender = MagicMock()
        checker = ForwardAttributionChecker(sender)
        payload = {
            "type": "text",
            "forward_origin": {"type": "channel", "chat_id": -1001, "message_id": 1},
        }
        config = self._config(allowlist=[SimpleNamespace(chat_id=-100999, name="Other")])
        assert await checker.should_use_native_forward(payload, config) is False

    @pytest.mark.asyncio
    async def test_allowlist_hit_with_admin(self):
        sender = MagicMock()
        sender.get_me = AsyncMock(return_value={"id": 777})
        sender.get_chat_member = AsyncMock(return_value={"status": "administrator", "can_post_messages": True})
        checker = ForwardAttributionChecker(sender)
        payload = {
            "type": "text",
            "forward_origin": {"type": "channel", "chat_id": -1001, "message_id": 1},
        }
        config = self._config(allowlist=[SimpleNamespace(chat_id=-1001, name="Origin")])
        assert await checker.should_use_native_forward(payload, config) is True

    @pytest.mark.asyncio
    async def test_runtime_admin_empty_allowlist(self):
        sender = MagicMock()
        sender.get_me = AsyncMock(return_value={"id": 777})
        sender.get_chat_member = AsyncMock(return_value={"status": "creator"})
        checker = ForwardAttributionChecker(sender)
        payload = {
            "type": "text",
            "forward_origin": {"type": "channel", "chat_id": -1001, "message_id": 1},
        }
        assert await checker.should_use_native_forward(payload, self._config()) is True

    @pytest.mark.asyncio
    async def test_not_admin(self):
        sender = MagicMock()
        sender.get_me = AsyncMock(return_value={"id": 777})
        sender.get_chat_member = AsyncMock(return_value={"status": "member"})
        checker = ForwardAttributionChecker(sender)
        payload = {
            "type": "text",
            "forward_origin": {"type": "channel", "chat_id": -1001, "message_id": 1},
        }
        assert await checker.should_use_native_forward(payload, self._config()) is False


class TestAttachNativeForwardFlag:
    def test_attaches_origin(self):
        payload = {
            "type": "text",
            "message_id": 1,
            "forward_origin": {"type": "channel", "chat_id": -1001, "message_id": 50},
        }
        attach_native_forward_flag(payload, True)
        assert payload["use_native_forward"] is True
        assert payload["native_forward_origin"]["chat_id"] == -1001
        assert payload["native_forward_origin"]["message_ids"] == [50]


class TestNativeForwardDelivery:
    @pytest.fixture
    def sender(self):
        return TelegramBotSender(bot_token="test:token")

    @pytest.mark.asyncio
    async def test_text_uses_forward_message(self, sender):
        sender.forward_message = AsyncMock(return_value=200)
        payload = {
            "message_id": 36,
            "use_native_forward": True,
            "native_forward_origin": {
                "chat_id": -100888,
                "message_id": 50,
                "message_ids": [50],
            },
        }
        result = await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="text",
            payload=payload,
            processed_payload={},
        )
        sender.forward_message.assert_awaited_once_with(
            chat_id=-1001,
            from_chat_id=-100888,
            message_id=50,
            reply_to_message_id=None,
        )
        assert result["sent_message_id"] == 200
        assert result["reply_mappings"] == [(36, 200)]

    @pytest.mark.asyncio
    async def test_album_uses_forward_messages(self, sender):
        sender.forward_messages = AsyncMock(return_value=[10, 11])
        payload = {
            "type": "album",
            "use_native_forward": True,
            "native_forward_origin": {
                "chat_id": -100888,
                "message_id": 50,
                "message_ids": [50, 51],
            },
            "items": [
                {"message_id": 1, "forward_origin": {"type": "channel", "chat_id": -100888, "message_id": 50}},
                {"message_id": 2, "forward_origin": {"type": "channel", "chat_id": -100888, "message_id": 51}},
            ],
        }
        result = await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="album",
            payload=payload,
            processed_payload={},
        )
        sender.forward_messages.assert_awaited_once_with(
            chat_id=-1001,
            from_chat_id=-100888,
            message_ids=[50, 51],
            reply_to_message_id=None,
        )
        assert result["sent_message_id"] == 10
        assert result["reply_mappings"] == [(1, 10), (2, 11)]

    @pytest.mark.asyncio
    async def test_without_native_flag_uses_send_message(self, sender):
        sender.send_message = AsyncMock(return_value=99)
        result = await sender.forward_to_destination(
            dest_chat_id=-1001,
            msg_type="text",
            payload={"message_id": 36, "text": "hello", "entities": []},
            processed_payload={"text_changed": False},
        )
        sender.send_message.assert_awaited_once()
        assert result["sent_message_id"] == 99


class TestReplacementsSkipNative:
    def test_build_processed_skips_replacements(self):
        from replacements import build_processed_payload

        config = MagicMock()
        config.get_active_destinations.return_value = [
            SimpleNamespace(chat_id=-1001, name="D", enabled=True)
        ]
        config.settings.replacement_rules = [
            SimpleNamespace(pattern="hello", replacement="bye", is_regex=False)
        ]
        payload = {
            "type": "text",
            "text": "hello world",
            "use_native_forward": True,
        }
        result = build_processed_payload(payload, config)
        assert "processed_text" not in result
        assert result["destinations"][0]["chat_id"] == -1001

    def test_needs_n8n_false_when_native(self):
        from replacements import needs_n8n

        assert needs_n8n({"type": "text", "text": "hi", "use_native_forward": True}) is False
