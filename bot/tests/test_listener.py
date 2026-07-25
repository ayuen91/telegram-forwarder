"""Tests for listener message classification and normalization."""

import sys
import types
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock


def _install_hydrogram_stub():
    """Minimal Hydrogram stub so listener helpers can be imported in unit tests."""
    if "hydrogram" in sys.modules and hasattr(sys.modules["hydrogram"], "Client"):
        return

    hydrogram = types.ModuleType("hydrogram")
    hydrogram.Client = MagicMock()
    hydrogram.filters = MagicMock()
    hydrogram.filters.chat = MagicMock()

    errors = types.ModuleType("hydrogram.errors")
    errors.FloodWait = Exception

    hydrogram_types = types.ModuleType("hydrogram.types")
    hydrogram_types.Message = MagicMock()

    sys.modules["hydrogram"] = hydrogram
    sys.modules["hydrogram.errors"] = errors
    sys.modules["hydrogram.types"] = hydrogram_types


_install_hydrogram_stub()

from listener import (
    _get_message_type,
    _serialize_contact,
    _serialize_location,
    _serialize_poll,
    _serialize_reply_markup,
    _serialize_venue,
    _source_message_ids,
    normalize_message,
)


def _msg(**attrs):
    """Build a minimal message-like object for type detection."""
    defaults = {
        "id": 1,
        "chat": SimpleNamespace(id=-100123),
        "text": None,
        "photo": None,
        "video": None,
        "document": None,
        "sticker": None,
        "voice": None,
        "video_note": None,
        "animation": None,
        "audio": None,
        "poll": None,
        "contact": None,
        "venue": None,
        "location": None,
        "pinned_message": None,
        "media_group_id": None,
        "reply_to_message_id": None,
        "reply_markup": None,
    }
    defaults.update(attrs)
    return SimpleNamespace(**defaults)


class TestGetMessageType:
    def test_venue_before_location(self):
        msg = _msg(venue=object(), location=object())
        assert _get_message_type(msg) == "venue"

    def test_poll(self):
        assert _get_message_type(_msg(poll=object())) == "poll"

    def test_contact(self):
        assert _get_message_type(_msg(contact=object())) == "contact"

    def test_location(self):
        assert _get_message_type(_msg(location=object())) == "location"

    def test_pin_message(self):
        msg = _msg(pinned_message=SimpleNamespace(id=99))
        assert _get_message_type(msg) == "pin"

    def test_unsupported_returns_none(self):
        assert _get_message_type(_msg()) is None


class TestSourceMessageIds:
    def test_poll_does_not_need_relay(self):
        assert _source_message_ids({"type": "poll", "message_id": 10}) == []

    def test_location_does_not_need_relay(self):
        assert _source_message_ids({"type": "location", "message_id": 11}) == []

    def test_venue_does_not_need_relay(self):
        assert _source_message_ids({"type": "venue", "message_id": 12}) == []

    def test_contact_does_not_need_relay(self):
        assert _source_message_ids({"type": "contact", "message_id": 13}) == []

    def test_pin_does_not_need_relay(self):
        assert _source_message_ids({"type": "pin", "message_id": 14}) == []

    def test_text_needs_relay(self):
        assert _source_message_ids({"type": "text", "message_id": 14}) == [14]

    def test_photo_needs_relay(self):
        assert _source_message_ids({"type": "photo", "message_id": 15}) == [15]


class TestSerialization:
    def test_serialize_poll_quiz(self):
        poll = SimpleNamespace(
            question="Pick one?",
            options=[SimpleNamespace(text="A"), SimpleNamespace(text="B")],
            is_anonymous=True,
            type=SimpleNamespace(value="quiz"),
            allows_multiple_answers=False,
            correct_option_id=1,
            explanation="Because B",
            open_period=60,
            close_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        data = _serialize_poll(poll)
        assert data["question"] == "Pick one?"
        assert data["options"] == ["A", "B"]
        assert data["type"] == "quiz"
        assert data["correct_option_id"] == 1
        assert data["explanation"] == "Because B"
        assert data["open_period"] == 60
        assert "2026-01-01" in data["close_date"]

    def test_serialize_location(self):
        loc = SimpleNamespace(latitude=1.23, longitude=4.56)
        assert _serialize_location(loc) == {
            "latitude": 1.23,
            "longitude": 4.56,
        }

    def test_serialize_venue(self):
        venue = SimpleNamespace(
            location=SimpleNamespace(latitude=10.0, longitude=20.0),
            title="Cafe",
            address="Main St",
            foursquare_id="fsq1",
            foursquare_type="food/cafe",
        )
        data = _serialize_venue(venue)
        assert data["title"] == "Cafe"
        assert data["latitude"] == 10.0
        assert data["foursquare_id"] == "fsq1"

    def test_serialize_contact(self):
        contact = SimpleNamespace(
            phone_number="+100",
            first_name="Ada",
            last_name="Lovelace",
            vcard="BEGIN:VCARD",
        )
        data = _serialize_contact(contact)
        assert data["phone_number"] == "+100"
        assert data["last_name"] == "Lovelace"
        assert data["vcard"] == "BEGIN:VCARD"

    def test_serialize_reply_markup(self):
        btn1 = SimpleNamespace(text="Google", url="https://google.com")
        btn2 = SimpleNamespace(text="Click", callback_data=b"data_123")
        markup = SimpleNamespace(inline_keyboard=[[btn1, btn2]])

        res = _serialize_reply_markup(markup)
        assert res == {
            "inline_keyboard": [
                [
                    {"text": "Google", "url": "https://google.com"},
                    {"text": "Click", "callback_data": "data_123"},
                ]
            ]
        }

    def test_normalize_message_pin(self):
        msg = _msg(
            id=500,
            chat=SimpleNamespace(id=-1001),
            pinned_message=SimpleNamespace(id=450),
        )
        payload = normalize_message(msg)
        assert payload["type"] == "pin"
        assert payload["message_id"] == 500
        assert payload["chat_id"] == -1001
        assert payload["pinned_message_id"] == 450

    def test_normalize_message_with_quote(self):
        quote = SimpleNamespace(
            text="Selected quoted text",
            html="<b>Selected quoted text</b>",
            position=12,
            entities=[object()],
        )
        msg = _msg(
            id=501,
            chat=SimpleNamespace(id=-1001),
            text=SimpleNamespace(html="Reply text"),
            reply_to_message_id=450,
            quote=quote,
        )
        payload = normalize_message(msg)
        assert payload["type"] == "text"
        assert payload["reply_to_message_id"] == 450
        assert payload["reply_to_quote_text"] == "Selected quoted text"
        assert payload["reply_to_quote_html"] == "<b>Selected quoted text</b>"
        assert payload["reply_to_quote_position"] == 12
