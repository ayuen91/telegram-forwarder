import asyncio
import sys
import types
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


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

    raw = types.ModuleType("hydrogram.raw")
    raw_types = types.ModuleType("hydrogram.raw.types")

    class UpdateNewChannelMessage:
        pass
    class UpdateNewMessage:
        pass
    class UpdateEditChannelMessage:
        pass
    class UpdateDeleteChannelMessages:
        pass
    class UpdateDeleteMessages:
        pass
    class UpdateChannelTooLong:
        pass
    class UpdatesTooLong:
        pass
    class InputPeerChannel:
        def __init__(self, channel_id=0, access_hash=0):
            self.channel_id = channel_id
            self.access_hash = access_hash
    class InputChannel:
        def __init__(self, channel_id=0, access_hash=0):
            self.channel_id = channel_id
            self.access_hash = access_hash
    class InputMessageID:
        def __init__(self, id=0):
            self.id = id

    raw_types.UpdateNewChannelMessage = UpdateNewChannelMessage
    raw_types.UpdateNewMessage = UpdateNewMessage
    raw_types.UpdateEditChannelMessage = UpdateEditChannelMessage
    raw_types.UpdateDeleteChannelMessages = UpdateDeleteChannelMessages
    raw_types.UpdateDeleteMessages = UpdateDeleteMessages
    raw_types.UpdateChannelTooLong = UpdateChannelTooLong
    raw_types.UpdatesTooLong = UpdatesTooLong
    raw_types.InputPeerChannel = InputPeerChannel
    raw_types.InputChannel = InputChannel
    raw_types.InputMessageID = InputMessageID

    raw_funcs = types.ModuleType("hydrogram.raw.functions")
    ch_raw = types.ModuleType("hydrogram.raw.functions.channels")
    ch_raw.GetMessages = MagicMock()
    msg_raw = types.ModuleType("hydrogram.raw.functions.messages")
    msg_raw.GetMessages = MagicMock()
    upd_raw = types.ModuleType("hydrogram.raw.functions.updates")
    upd_raw.GetState = MagicMock()

    sys.modules["hydrogram"] = hydrogram
    sys.modules["hydrogram.errors"] = errors
    sys.modules["hydrogram.types"] = hydrogram_types
    sys.modules["hydrogram.raw"] = raw
    sys.modules["hydrogram.raw.types"] = raw_types
    sys.modules["hydrogram.raw.functions"] = raw_funcs
    sys.modules["hydrogram.raw.functions.channels"] = ch_raw
    sys.modules["hydrogram.raw.functions.messages"] = msg_raw
    sys.modules["hydrogram.raw.functions.updates"] = upd_raw


_install_hydrogram_stub()

from listener import (
    _get_message_type,
    _is_native_forward_fallback_error,
    _serialize_contact,
    _serialize_location,
    _serialize_poll,
    _serialize_reply_markup,
    _serialize_venue,
    _source_message_ids,
    _extract_reply_quote,
    _extract_quote_from_tl_reply_header,
    _capture_tl_message_quote,
    _resolve_raw_quote_if_needed,
    normalize_message,
    forward_message_pipeline,
    _process_edit_event,
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

    def test_normalize_message_with_raw_tl_quote(self):
        raw_header = SimpleNamespace(
            quote_text="Quoted text from raw TL",
            quote_offset=8,
        )
        raw_msg = SimpleNamespace(reply_to=raw_header)
        msg = _msg(
            id=502,
            chat=SimpleNamespace(id=-1001),
            text=SimpleNamespace(html="Reply text"),
            reply_to_message_id=450,
            quote=None,
            reply_to=None,
            _raw=raw_msg,
        )
        payload = normalize_message(msg)
        assert payload["type"] == "text"
        assert payload["reply_to_message_id"] == 450
        assert payload["reply_to_quote_text"] == "Quoted text from raw TL"
        assert payload["reply_to_quote_html"] == "Quoted text from raw TL"
        assert payload["reply_to_quote_position"] == 8

    def test_extract_reply_quote_with_entities(self):
        raw_header = SimpleNamespace(
            quote_text="Bold quote",
            quote_offset=3,
            quote_entities=[SimpleNamespace(type="bold", offset=0, length=4)],
        )
        msg = SimpleNamespace(
            quote=None,
            quote_text=None,
            reply_to=raw_header,
            reply_to_header=None,
            _raw=None,
        )
        plain, html, pos = _extract_reply_quote(msg)
        assert plain == "Bold quote"
        assert pos == 3
        # Without hydrogram parser in test env, html falls back to plain text
        assert html == "Bold quote"

    def test_extract_reply_quote_nested_on_reply_to(self):
        nested = SimpleNamespace(
            text="Nested quote",
            html="<i>Nested quote</i>",
            position=5,
            entities=[object()],
        )
        reply_to = SimpleNamespace(quote=nested, quote_text=None)
        msg = SimpleNamespace(
            quote=None,
            quote_text=None,
            reply_to=reply_to,
            reply_to_header=None,
            _raw=None,
        )
        plain, html, pos = _extract_reply_quote(msg)
        assert plain == "Nested quote"
        assert html == "<i>Nested quote</i>"
        assert pos == 5

    def test_extract_quote_from_tl_reply_header_boolean_quote_flag(self):
        """TL MessageReplyHeader.quote is a bool flag, not a nested object."""
        header = SimpleNamespace(
            quote=True,
            quote_text="Album caption tests",
            quote_offset=0,
            quote_entities=None,
        )
        data = _extract_quote_from_tl_reply_header(header)
        assert data is not None
        assert data["reply_to_quote_text"] == "Album caption tests"
        assert data["reply_to_quote_position"] == 0

    def test_capture_tl_message_quote_channel_filter(self):
        tl_msg = SimpleNamespace(
            id=595,
            peer_id=SimpleNamespace(channel_id=4497139985),
            reply_to=SimpleNamespace(
                quote=True,
                quote_text="Selected part",
                quote_offset=6,
            ),
        )
        quote = _capture_tl_message_quote(tl_msg, -1004497139985)
        assert quote is not None
        assert quote["_source_message_id"] == 595
        assert quote["reply_to_quote_text"] == "Selected part"

        wrong_channel = _capture_tl_message_quote(tl_msg, -1009999999999)
        assert wrong_channel is None


class TestNativeForwardFallbackError:
    """_is_native_forward_fallback_error should match all access-denial errors."""

    # ── errors present before this fix ──────────────────────────────────
    def test_protected(self):
        assert _is_native_forward_fallback_error(Exception("Content is protected"))

    def test_cannot_be_forwarded(self):
        assert _is_native_forward_fallback_error(Exception("cannot be forwarded"))

    def test_message_not_found(self):
        assert _is_native_forward_fallback_error(Exception("message not found"))

    def test_chat_not_found(self):
        assert _is_native_forward_fallback_error(Exception("chat not found"))

    def test_not_enough_rights(self):
        assert _is_native_forward_fallback_error(Exception("not enough rights to post"))

    def test_bot_is_not_a_member(self):
        assert _is_native_forward_fallback_error(Exception("Bot is not a member of the channel"))

    def test_forbidden(self):
        assert _is_native_forward_fallback_error(Exception("Forbidden: bot is not a member"))

    def test_forwardmessage_failed(self):
        assert _is_native_forward_fallback_error(Exception("Bot API forwardMessage failed: ..."))

    # ── new keywords added by this fix ───────────────────────────────────
    def test_channel_private(self):
        assert _is_native_forward_fallback_error(Exception("CHANNEL_PRIVATE"))

    def test_user_not_participant(self):
        assert _is_native_forward_fallback_error(Exception("USER_NOT_PARTICIPANT"))

    def test_not_a_member(self):
        assert _is_native_forward_fallback_error(Exception("Bot is not a member of channel"))

    def test_bot_was_kicked(self):
        assert _is_native_forward_fallback_error(Exception("bot was kicked from the channel"))

    def test_kicked_from(self):
        assert _is_native_forward_fallback_error(Exception("Kicked from supergroup"))

    def test_peer_id_invalid(self):
        assert _is_native_forward_fallback_error(Exception("PEER_ID_INVALID"))

    def test_need_administrator_rights(self):
        assert _is_native_forward_fallback_error(Exception("Need administrator rights in the channel"))

    def test_administrator_rights(self):
        assert _is_native_forward_fallback_error(Exception("Bad Request: administrator rights required"))

    # ── unrelated errors must NOT match ─────────────────────────────────
    def test_unrelated_error_returns_false(self):
        assert not _is_native_forward_fallback_error(Exception("network timeout"))

    def test_flood_wait_returns_false(self):
        assert not _is_native_forward_fallback_error(Exception("Too Many Requests: retry after 30"))


class TestQuoteExtractionAndCatchup:
    def test_capture_tl_message_quote_direct(self):
        from listener import _capture_tl_message_quote
        reply_to = SimpleNamespace(
            quote=None,
            quote_text="Important quotation",
            quote_offset=12,
            quote_entities=None,
        )
        peer = SimpleNamespace(channel_id=123456789)
        tl_msg = SimpleNamespace(
            id=101,
            peer_id=peer,
            reply_to=reply_to,
        )

        quote = _capture_tl_message_quote(tl_msg, -100123456789)
        assert quote is not None
        assert quote["reply_to_quote_text"] == "Important quotation"
        assert quote["reply_to_quote_position"] == 12
        assert quote["_source_message_id"] == 101

    @pytest.mark.asyncio
    async def test_resolve_raw_quote_if_needed(self):
        from listener import _resolve_raw_quote_if_needed
        client = MagicMock()
        client.resolve_peer = AsyncMock(return_value=SimpleNamespace())
        reply_to = SimpleNamespace(
            quote=None,
            quote_text="Resolved catchup quote",
            quote_offset=0,
            quote_entities=None,
        )
        raw_msg = SimpleNamespace(reply_to=reply_to)
        res = SimpleNamespace(messages=[raw_msg])
        client.invoke = AsyncMock(return_value=res)

        payload = {"message_id": 55, "reply_to_message_id": 40}
        await _resolve_raw_quote_if_needed(client, -100123456789, 55, payload)
        assert payload.get("reply_to_quote_text") == "Resolved catchup quote"
        assert payload.get("reply_to_quote_position") == 0

    @pytest.mark.asyncio
    async def test_on_raw_update_container_unpacking(self):
        from listener import register_listener, UpdateNewChannelMessage
        app = MagicMock()
        registered_raw = []
        def mock_on_raw():
            def decorator(fn):
                registered_raw.append(fn)
                return fn
            return decorator
        app.on_raw_update = mock_on_raw
        app.on_message = MagicMock(return_value=lambda fn: fn)
        app.on_edited_message = MagicMock(return_value=lambda fn: fn)

        queue = asyncio.Queue()
        redis_mock = AsyncMock()
        register_listener(app, -100123456789, queue, redis_client=redis_mock)
        assert len(registered_raw) == 1
        raw_handler = registered_raw[0]

        reply_to = SimpleNamespace(
            quote=None,
            quote_text="Container quote",
            quote_offset=5,
            quote_entities=None,
        )
        peer = SimpleNamespace(channel_id=123456789)
        tl_msg = SimpleNamespace(
            id=77,
            peer_id=peer,
            reply_to=reply_to,
        )

        if UpdateNewChannelMessage is not None:
            nested_update = UpdateNewChannelMessage()
            nested_update.message = tl_msg
        else:
            nested_update = SimpleNamespace(message=tl_msg)

        container_update = SimpleNamespace(updates=[nested_update])

        await raw_handler(app, container_update, None, None)
        redis_mock.setex.assert_awaited_once()
        call_args = redis_mock.setex.call_args[0]
        assert "listener:quote:-100123456789:77" in call_args[0]
        assert "Container quote" in call_args[2]


class TestFloodWaitPropagation:
    @pytest.mark.asyncio
    async def test_forward_message_pipeline_propagates_flood_wait(self, monkeypatch):
        from listener import forward_message_pipeline
        from hydrogram.errors import FloodWait

        async def mock_relay(*args, **kwargs):
            fw = FloodWait(543)
            fw.value = 543
            raise fw

        monkeypatch.setattr("listener.relay_to_bot", mock_relay)

        sender = MagicMock()
        payload = {"message_id": 100, "chat_id": -100, "has_media": True, "type": "photo", "file_id": "xyz"}
        processed = {"destinations": [{"chat_id": -200, "name": "Dest"}]}

        with pytest.raises(FloodWait):
            await forward_message_pipeline(
                sender,
                payload,
                processed,
                ":memory:",
                hydrogram_app=MagicMock(),
                relay=MagicMock(),
            )

    @pytest.mark.asyncio
    async def test_process_payload_propagates_flood_wait(self, monkeypatch):
        from listener import process_payload
        from hydrogram.errors import FloodWait

        async def mock_pipeline(*args, **kwargs):
            fw = FloodWait(300)
            fw.value = 300
            raise fw

        monkeypatch.setattr("listener.forward_message_pipeline", mock_pipeline)
        monkeypatch.setattr("listener._resolve_processed_payload", lambda cfg, p: {"destinations": [{"chat_id": -200}]})

        sender = MagicMock()
        qm = MagicMock()
        qm.enqueue = AsyncMock()
        qm.remove_from_queue = AsyncMock()
        qm.enqueue_failed = AsyncMock()

        payload = {"message_id": 100, "chat_id": -100, "type": "text", "text": "hello"}
        config = MagicMock()

        with pytest.raises(FloodWait):
            await process_payload(sender, qm, payload, ":memory:", config)

    @pytest.mark.asyncio
    async def test_process_payload_skips_in_flight_deleted_tombstone(self):
        from listener import process_payload

        sender = MagicMock()
        qm = MagicMock()
        qm.enqueue = AsyncMock()
        qm.remove_from_queue = AsyncMock()

        dedup = MagicMock()
        dedup.is_deleted = AsyncMock(return_value=True)

        payload = {"message_id": 100, "chat_id": -100123, "type": "text", "text": "hello"}
        config = MagicMock()

        res = await process_payload(sender, qm, payload, ":memory:", config, dedup=dedup)
        assert res == "success"
        qm.enqueue.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_deletion_event_deletes_album_sub_items(self, tmp_path):
        import aiosqlite
        from listener import _process_deletion_event

        db_path = str(tmp_path / "test_del.db")
        async with aiosqlite.connect(db_path) as db:
            await db.execute("""
                CREATE TABLE messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_message_id INTEGER,
                    source_chat_id INTEGER,
                    status TEXT,
                    deleted_at DATETIME
                )
            """)
            await db.execute("""
                CREATE TABLE message_destinations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER,
                    destination_chat_id INTEGER,
                    sent_message_id INTEGER,
                    status TEXT,
                    sent_at DATETIME
                )
            """)
            await db.execute("""
                CREATE TABLE message_reply_map (
                    source_chat_id INTEGER,
                    source_message_id INTEGER,
                    destination_chat_id INTEGER,
                    sent_message_id INTEGER,
                    PRIMARY KEY (source_chat_id, source_message_id, destination_chat_id)
                )
            """)
            # Album item 1
            await db.execute("INSERT INTO messages (id, telegram_message_id, source_chat_id, status) VALUES (1, 501, -100123, 'received')")
            await db.execute("INSERT INTO message_destinations (message_id, destination_chat_id, sent_message_id, status) VALUES (1, -100999, 901, 'sent')")
            await db.execute("INSERT INTO message_reply_map VALUES (-100123, 501, -100999, 901)")
            # Album item 2 (stored only in message_reply_map)
            await db.execute("INSERT INTO message_reply_map VALUES (-100123, 502, -100999, 902)")
            await db.commit()

        sender = MagicMock()
        sender.delete_message = AsyncMock()

        dest_obj = MagicMock()
        dest_obj.chat_id = -100999
        dest_obj.name = "Dest 1"
        config = MagicMock()
        config.get_active_destinations.return_value = [dest_obj]

        dedup = MagicMock()
        dedup.mark_deleted = AsyncMock()

        # Delete both item 501 and item 502
        await _process_deletion_event(
            source_chat_id=-100123,
            message_ids=[501, 502],
            db_path=db_path,
            sender=sender,
            config=config,
            dedup=dedup,
        )

        dedup.mark_deleted.assert_called_once_with(-100123, [501, 502])
        assert sender.delete_message.call_count == 2
        sender.delete_message.assert_any_call(-100999, 901)
        sender.delete_message.assert_any_call(-100999, 902)

        # Check DB cleanup
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT status FROM messages WHERE telegram_message_id = 501")
            m_row = await cursor.fetchone()
            assert m_row["status"] == "deleted"

            cursor = await db.execute("SELECT COUNT(*) as cnt FROM message_reply_map")
            cnt_row = await cursor.fetchone()
            assert cnt_row["cnt"] == 0

    @pytest.mark.asyncio
    async def test_compute_content_hash_behavior(self):
        from listener import _compute_content_hash

        p1 = {"type": "text", "text_html": "Hello World"}
        p2 = {"type": "text", "text_html": "Hello World"}
        p3 = {"type": "text", "text_html": "Hello World Edited"}

        h1 = _compute_content_hash(p1)
        h2 = _compute_content_hash(p2)
        h3 = _compute_content_hash(p3)

        assert h1 == h2
        assert h1 != h3
        assert len(h1) == 64

    @pytest.mark.asyncio
    async def test_process_edit_event_preserves_unmapped_quote(self, tmp_path):
        import aiosqlite
        from listener import _process_edit_event

        db_path = str(tmp_path / "test_edit.db")
        async with aiosqlite.connect(db_path) as db:
            await db.execute("""
                CREATE TABLE messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_message_id INTEGER,
                    source_chat_id INTEGER,
                    processed_text TEXT,
                    processed_caption TEXT,
                    processed_at DATETIME
                )
            """)
            await db.execute("""
                CREATE TABLE message_reply_map (
                    source_chat_id INTEGER,
                    source_message_id INTEGER,
                    destination_chat_id INTEGER,
                    sent_message_id INTEGER,
                    PRIMARY KEY (source_chat_id, source_message_id, destination_chat_id)
                )
            """)
            await db.execute("INSERT INTO messages (id, telegram_message_id, source_chat_id) VALUES (1, 100, -100123)")
            await db.execute("INSERT INTO message_reply_map VALUES (-100123, 100, -100999, 555)")
            await db.commit()

        sender = MagicMock()
        sender.edit_message_text = AsyncMock()

        dest_obj = MagicMock()
        dest_obj.chat_id = -100999
        dest_obj.name = "Dest 1"
        config = MagicMock()
        config.get_active_destinations.return_value = [dest_obj]
        config.get_rules_for_destination.return_value = []

        payload = {
            "type": "text",
            "text": "New body text",
            "reply_to_quote_text": "Original quote text",
            "reply_to_message_id": 9999,  # unmapped reply
        }

        await _process_edit_event(
            source_chat_id=-100123,
            message_id=100,
            payload=payload,
            db_path=db_path,
            sender=sender,
            config=config,
        )

        sender.edit_message_text.assert_called_once()
        args, kwargs = sender.edit_message_text.call_args
        assert args[0] == -100999
        assert args[1] == 555
        assert "<blockquote expandable>Original quote text</blockquote>\n\nNew body text" in args[2]


class TestResolveRawQuoteIfNeeded:
    @pytest.mark.asyncio
    async def test_input_peer_channel_converted_to_input_channel(self):
        import hydrogram.raw.types as _raw_types
        import hydrogram.raw.functions.channels as _ch_raw

        peer = _raw_types.InputPeerChannel(channel_id=123456789, access_hash=987654321)
        client = MagicMock()
        client.resolve_peer = AsyncMock(return_value=peer)

        # Mock reply_to TL header with quote
        reply_to_header = SimpleNamespace(
            quote=True,
            quote_text="Quoted text from large channel",
            quote_offset=15,
            quote_entities=None,
        )
        raw_msg = SimpleNamespace(id=50, reply_to=reply_to_header)
        res = SimpleNamespace(messages=[raw_msg])
        client.invoke = AsyncMock(return_value=res)

        payload = {
            "reply_to_message_id": 40,
            "reply_to_quote_text": None,
        }

        await _resolve_raw_quote_if_needed(
            client=client,
            source_chat_id=-100123456789,
            message_id=50,
            payload=payload,
        )

        client.invoke.assert_called_once()
        # Verify channels.GetMessages was called with InputChannel
        assert _ch_raw.GetMessages.called
        channel_arg = _ch_raw.GetMessages.call_args.kwargs["channel"]
        assert isinstance(channel_arg, _raw_types.InputChannel)
        assert channel_arg.channel_id == 123456789
        assert channel_arg.access_hash == 987654321

        # Verify quote data extracted into payload
        assert payload["reply_to_quote_text"] == "Quoted text from large channel"
        assert payload["reply_to_quote_position"] == 15

    @pytest.mark.asyncio
    async def test_input_channel_directly_passed(self):
        import hydrogram.raw.types as _raw_types
        import hydrogram.raw.functions.channels as _ch_raw

        peer = _raw_types.InputChannel(channel_id=123456789, access_hash=987654321)
        client = MagicMock()
        client.resolve_peer = AsyncMock(return_value=peer)

        reply_to_header = SimpleNamespace(
            quote=True,
            quote_text="Quoted text directly from input channel",
            quote_offset=0,
            quote_entities=None,
        )
        raw_msg = SimpleNamespace(id=51, reply_to=reply_to_header)
        res = SimpleNamespace(messages=[raw_msg])
        client.invoke = AsyncMock(return_value=res)

        payload = {
            "reply_to_message_id": 41,
            "reply_to_quote_text": None,
        }

        await _resolve_raw_quote_if_needed(
            client=client,
            source_chat_id=-100123456789,
            message_id=51,
            payload=payload,
        )

        assert payload["reply_to_quote_text"] == "Quoted text directly from input channel"
        assert payload["reply_to_quote_position"] == 0

    @pytest.mark.asyncio
    async def test_no_reply_id_skips_call(self):
        client = MagicMock()
        client.resolve_peer = AsyncMock()
        payload = {
            "reply_to_message_id": None,
            "reply_to_quote_text": None,
        }
        await _resolve_raw_quote_if_needed(
            client=client,
            source_chat_id=-100123,
            message_id=50,
            payload=payload,
        )
        client.resolve_peer.assert_not_called()

    @pytest.mark.asyncio
    async def test_already_has_quote_skips_call(self):
        client = MagicMock()
        client.resolve_peer = AsyncMock()
        payload = {
            "reply_to_message_id": 40,
            "reply_to_quote_text": "Already present",
        }
        await _resolve_raw_quote_if_needed(
            client=client,
            source_chat_id=-100123,
            message_id=50,
            payload=payload,
        )
        client.resolve_peer.assert_not_called()


@pytest.mark.asyncio
class TestPipelineLinkRewriting:
    @pytest.fixture
    def db_path(self, tmp_path):
        return str(tmp_path / "test_pipeline.db")

    async def _setup_db(self, path):
        import aiosqlite
        async with aiosqlite.connect(path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_message_id INTEGER NOT NULL,
                    source_chat_id INTEGER NOT NULL,
                    message_type TEXT NOT NULL,
                    original_text TEXT,
                    original_caption TEXT,
                    processed_text TEXT,
                    processed_caption TEXT,
                    has_media INTEGER DEFAULT 0,
                    media_group_id TEXT,
                    album_item_count INTEGER,
                    status TEXT NOT NULL DEFAULT 'received',
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    received_at TEXT DEFAULT (datetime('now')),
                    processed_at TEXT,
                    sent_at TEXT,
                    deleted_at TEXT,
                    UNIQUE(telegram_message_id, source_chat_id)
                );
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS message_destinations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER NOT NULL REFERENCES messages(id),
                    destination_chat_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    sent_message_id INTEGER,
                    sent_at TEXT,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    UNIQUE(message_id, destination_chat_id)
                );
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS message_reply_map (
                    source_chat_id INTEGER NOT NULL,
                    source_message_id INTEGER NOT NULL,
                    destination_chat_id INTEGER NOT NULL,
                    sent_message_id INTEGER NOT NULL,
                    created_at TEXT DEFAULT (datetime('now')),
                    PRIMARY KEY (source_chat_id, source_message_id, destination_chat_id)
                );
                """
            )
            # Insert mapping: source msg 11808 in dest -1002674892914 -> 5432
            await db.execute(
                """
                INSERT INTO message_reply_map (source_chat_id, source_message_id, destination_chat_id, sent_message_id)
                VALUES (?, ?, ?, ?)
                """,
                (-1001707179235, 11808, -1002674892914, 5432),
            )
            # Insert mapping: source msg 11808 in dest -1005555555555 -> 8888
            await db.execute(
                """
                INSERT INTO message_reply_map (source_chat_id, source_message_id, destination_chat_id, sent_message_id)
                VALUES (?, ?, ?, ?)
                """,
                (-1001707179235, 11808, -1005555555555, 8888),
            )
            await db.commit()

    async def test_forward_pipeline_rewrites_internal_hyperlink(self, db_path):
        from unittest.mock import patch
        from media_relay import RelayConfig
        await self._setup_db(db_path)

        sender = MagicMock()
        delivered_payloads = []

        async def _mock_forward(**kwargs):
            delivered_payloads.append(dict(kwargs["processed_payload"]))
            return {
                "sent_message_id": 9001,
                "reply_mappings": [(kwargs["payload"]["message_id"], 9001)],
            }

        sender.forward_to_destination = AsyncMock(side_effect=_mock_forward)

        async def _call_mock(fn):
            return await fn()

        sender.call_with_flood_wait = AsyncMock(side_effect=_call_mock)


        payload = {
            "type": "text",
            "message_id": 11850,
            "chat_id": -1001707179235,
            "text": "For more info follow this post",
            "text_html": 'For more info <a href="https://t.me/c/1707179235/11808">follow</a> this post',
        }
        processed_payload = {
            **payload,
            "text_changed": False,
            "destinations": [
                {"chat_id": -1002674892914, "name": "Dest A", "enabled": True},
                {"chat_id": -1005555555555, "name": "Dest B", "enabled": True},
            ],
        }

        with patch("listener.relay_to_bot", new=AsyncMock(return_value=[11850])), \
             patch("listener.cleanup_relay", new=AsyncMock()):
            status = await forward_message_pipeline(
                sender=sender,
                payload=payload,
                processed_payload=processed_payload,
                db_path=db_path,
                hydrogram_app=MagicMock(),
                relay=RelayConfig(userbot_target=123, bot_from_chat=123),
            )


        assert status == "success"
        assert len(delivered_payloads) == 2


        # Destination A payload
        p_a = delivered_payloads[0]
        assert p_a["text_changed"] is True
        assert 'href="https://t.me/c/2674892914/5432"' in p_a["processed_text_html"]

        # Destination B payload
        p_b = delivered_payloads[1]
        assert p_b["text_changed"] is True
        assert 'href="https://t.me/c/5555555555/8888"' in p_b["processed_text_html"]

    async def test_process_edit_event_rewrites_internal_hyperlink(self, db_path):
        import aiosqlite
        await self._setup_db(db_path)

        # Map msg 11850 in Dest A -> 9001
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                """
                INSERT INTO message_reply_map (source_chat_id, source_message_id, destination_chat_id, sent_message_id)
                VALUES (?, ?, ?, ?)
                """,
                (-1001707179235, 11850, -1002674892914, 9001),
            )
            await db.commit()

        sender = MagicMock()
        sender.edit_message_text = AsyncMock()

        config = MagicMock()
        config.get_active_destinations.return_value = [
            SimpleNamespace(chat_id=-1002674892914, name="Dest A", enabled=True, username="")
        ]
        config.settings.replacement_rules = []

        payload = {
            "type": "text",
            "message_id": 11850,
            "chat_id": -1001707179235,
            "text": "Edited text follow",
            "text_html": 'Edited text <a href="https://t.me/c/1707179235/11808">follow</a>',
        }

        await _process_edit_event(
            source_chat_id=-1001707179235,
            message_id=11850,
            payload=payload,
            db_path=db_path,
            sender=sender,
            config=config,
        )

        sender.edit_message_text.assert_called_once()
        call_args = sender.edit_message_text.call_args[0]
        dest_id, sent_id, text = call_args[0], call_args[1], call_args[2]
        assert dest_id == -1002674892914
        assert sent_id == 9001
        assert '<a href="https://t.me/c/2674892914/5432">follow</a>' in text

    async def test_forward_pipeline_rewrites_public_dest_and_inline_keyboard(self, db_path):
        from unittest.mock import patch
        from media_relay import RelayConfig
        await self._setup_db(db_path)

        sender = MagicMock()
        delivered_payloads = []

        async def _mock_forward(**kwargs):
            delivered_payloads.append(dict(kwargs["processed_payload"]))
            return {
                "sent_message_id": 9002,
                "reply_mappings": [(kwargs["payload"]["message_id"], 9002)],
            }

        sender.forward_to_destination = AsyncMock(side_effect=_mock_forward)
        async def _call_mock(fn):
            return await fn()
        sender.call_with_flood_wait = AsyncMock(side_effect=_call_mock)

        payload = {
            "type": "text",
            "message_id": 11851,
            "chat_id": -1001707179235,
            "text": "Check out our previous post",
            "text_html": 'Check out our <a href="https://t.me/c/1707179235/11808">previous post</a>',
            "reply_markup": {
                "inline_keyboard": [
                    [{"text": "Post Link", "url": "https://t.me/c/1707179235/11808"}]
                ]
            },
        }
        processed_payload = {
            **payload,
            "text_changed": False,
            "destinations": [
                {"chat_id": -1002674892914, "name": "Public Dest", "username": "public_channel", "enabled": True},
            ],
        }

        with patch("listener.relay_to_bot", new=AsyncMock(return_value=[11851])), \
             patch("listener.cleanup_relay", new=AsyncMock()):
            status = await forward_message_pipeline(
                sender=sender,
                payload=payload,
                processed_payload=processed_payload,
                db_path=db_path,
                hydrogram_app=MagicMock(),
                relay=RelayConfig(userbot_target=123, bot_from_chat=123),
            )

        assert status == "success"
        assert len(delivered_payloads) == 1
        p = delivered_payloads[0]
        assert 'href="https://t.me/public_channel/5432"' in p["processed_text_html"]
        assert p["reply_markup"]["inline_keyboard"][0][0]["url"] == "https://t.me/public_channel/5432"

    async def test_forward_pipeline_unmapped_links_preserved(self, db_path):
        from unittest.mock import patch
        from media_relay import RelayConfig
        await self._setup_db(db_path)

        sender = MagicMock()
        delivered_payloads = []

        async def _mock_forward(**kwargs):
            delivered_payloads.append(dict(kwargs["processed_payload"]))
            return {
                "sent_message_id": 9003,
                "reply_mappings": [(kwargs["payload"]["message_id"], 9003)],
            }

        sender.forward_to_destination = AsyncMock(side_effect=_mock_forward)
        async def _call_mock(fn):
            return await fn()
        sender.call_with_flood_wait = AsyncMock(side_effect=_call_mock)

        payload = {
            "type": "text",
            "message_id": 11852,
            "chat_id": -1001707179235,
            "text": "Link to unmapped post https://t.me/c/1707179235/99999",
            "text_html": 'Link to unmapped post <a href="https://t.me/c/1707179235/99999">unmapped</a>',
        }
        processed_payload = {
            **payload,
            "text_changed": False,
            "destinations": [
                {"chat_id": -1002674892914, "name": "Dest A", "enabled": True},
            ],
        }

        with patch("listener.relay_to_bot", new=AsyncMock(return_value=[11852])), \
             patch("listener.cleanup_relay", new=AsyncMock()):
            status = await forward_message_pipeline(
                sender=sender,
                payload=payload,
                processed_payload=processed_payload,
                db_path=db_path,
                hydrogram_app=MagicMock(),
                relay=RelayConfig(userbot_target=123, bot_from_chat=123),
            )

        assert status == "success"
        p = delivered_payloads[0]
        # Unmapped link is preserved as-is
        assert 'href="https://t.me/c/1707179235/99999"' in (p.get("processed_text_html") or p.get("text_html"))

    async def test_forward_pipeline_multiple_links_in_single_message(self, db_path):
        import aiosqlite
        from unittest.mock import patch
        from media_relay import RelayConfig
        await self._setup_db(db_path)

        # Insert second mapping: msg 11809 -> 5433 in Dest A
        async with aiosqlite.connect(db_path) as db:
            await db.execute(
                """
                INSERT INTO message_reply_map (source_chat_id, source_message_id, destination_chat_id, sent_message_id)
                VALUES (?, ?, ?, ?)
                """,
                (-1001707179235, 11809, -1002674892914, 5433),
            )
            await db.commit()

        sender = MagicMock()
        delivered_payloads = []

        async def _mock_forward(**kwargs):
            delivered_payloads.append(dict(kwargs["processed_payload"]))
            return {
                "sent_message_id": 9004,
                "reply_mappings": [(kwargs["payload"]["message_id"], 9004)],
            }

        sender.forward_to_destination = AsyncMock(side_effect=_mock_forward)
        async def _call_mock(fn):
            return await fn()
        sender.call_with_flood_wait = AsyncMock(side_effect=_call_mock)

        payload = {
            "type": "text",
            "message_id": 11853,
            "chat_id": -1001707179235,
            "text": "Check part 1 and part 2",
            "text_html": 'Check <a href="https://t.me/c/1707179235/11808">part 1</a> and <a href="https://t.me/c/1707179235/11809?single">part 2</a>',
        }
        processed_payload = {
            **payload,
            "text_changed": False,
            "destinations": [
                {"chat_id": -1002674892914, "name": "Dest A", "enabled": True},
            ],
        }

        with patch("listener.relay_to_bot", new=AsyncMock(return_value=[11853])), \
             patch("listener.cleanup_relay", new=AsyncMock()):
            status = await forward_message_pipeline(
                sender=sender,
                payload=payload,
                processed_payload=processed_payload,
                db_path=db_path,
                hydrogram_app=MagicMock(),
                relay=RelayConfig(userbot_target=123, bot_from_chat=123),
            )

        assert status == "success"
        p = delivered_payloads[0]
        assert 'href="https://t.me/c/2674892914/5432"' in p["processed_text_html"]
        assert 'href="https://t.me/c/2674892914/5433?single"' in p["processed_text_html"]






