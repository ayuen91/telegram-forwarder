"""Tests for listener message classification and normalization."""

import sys
import types
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock


def _install_pyrogram_stub():
    """Minimal Pyrogram stub so listener helpers can be imported in unit tests."""
    if "pyrogram" in sys.modules and hasattr(sys.modules["pyrogram"], "Client"):
        return

    pyrogram = types.ModuleType("pyrogram")
    pyrogram.Client = MagicMock()
    pyrogram.filters = MagicMock()
    pyrogram.filters.chat = MagicMock()

    errors = types.ModuleType("pyrogram.errors")
    errors.FloodWait = Exception

    pyrogram_types = types.ModuleType("pyrogram.types")
    pyrogram_types.Message = MagicMock()

    sys.modules["pyrogram"] = pyrogram
    sys.modules["pyrogram.errors"] = errors
    sys.modules["pyrogram.types"] = pyrogram_types


_install_pyrogram_stub()

from listener import (
    _get_message_type,
    _serialize_contact,
    _serialize_location,
    _serialize_poll,
    _serialize_venue,
    _source_message_ids,
)


def _msg(**attrs):
    """Build a minimal message-like object for type detection."""
    defaults = {
        "id": 1,
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

    def test_text_does_not_need_relay(self):
        assert _source_message_ids({"type": "text", "message_id": 14}) == []

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
