"""Tests for TelegramBotSender helpers (no live API calls)."""

import pytest

from telegram_sender import TelegramBotSender


class TestTelegramBotSenderHelpers:
    def test_reply_params_none(self):
        assert TelegramBotSender._reply_params(None) is None

    def test_reply_params_message_id(self):
        assert TelegramBotSender._reply_params(42) == {"message_id": 42}

    def test_reply_params_string_id(self):
        assert TelegramBotSender._reply_params("99") == {"message_id": 99}
