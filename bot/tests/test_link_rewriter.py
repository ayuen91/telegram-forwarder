"""Unit and integration tests for internal Telegram message link rewriter."""

import pytest
import aiosqlite
from link_rewriter import (
    normalize_channel_id_for_url,
    channel_id_candidates,
    extract_internal_telegram_links,
    format_destination_url,
    lookup_sent_message_id,
    rewrite_text_links,
    rewrite_reply_markup_links,
    resolve_destination_link_map,
    rewrite_payload_for_destination,
)


class TestChannelNormalization:
    def test_normalize_channel_id_for_url(self):
        assert normalize_channel_id_for_url(-1001707179235) == "1707179235"
        assert normalize_channel_id_for_url("-1001707179235") == "1707179235"
        assert normalize_channel_id_for_url(-1707179235) == "1707179235"
        assert normalize_channel_id_for_url(1707179235) == "1707179235"
        assert normalize_channel_id_for_url("1707179235") == "1707179235"

    def test_channel_id_candidates(self):
        cands = channel_id_candidates("1707179235")
        assert -1001707179235 in cands
        assert -1707179235 in cands
        assert 1707179235 in cands

        cands_neg = channel_id_candidates(-1001707179235)
        assert -1001707179235 in cands_neg


class TestExtractLinks:
    def test_extract_private_post_links(self):
        text = 'Click here: <a href="https://t.me/c/1707179235/11808">follow</a> and https://t.me/c/1707179235/11809?single'
        matches = extract_internal_telegram_links(text)
        assert len(matches) == 2

        m1 = matches[0]
        assert m1.channel_ref == "1707179235"
        assert m1.message_id == 11808
        assert m1.raw_url == "https://t.me/c/1707179235/11808"

        m2 = matches[1]
        assert m2.channel_ref == "1707179235"
        assert m2.message_id == 11809
        assert m2.extra == "?single"
        assert m2.raw_url == "https://t.me/c/1707179235/11809?single"

    def test_extract_topic_private_link(self):
        text = 'Check thread: https://t.me/c/1707179235/50/11808'
        matches = extract_internal_telegram_links(text)
        assert len(matches) == 1
        assert matches[0].channel_ref == "1707179235"
        assert matches[0].thread_id == 50
        assert matches[0].message_id == 11808

    def test_extract_tg_privatepost(self):
        text = 'tg://privatepost?channel=1707179235&post=11808'
        matches = extract_internal_telegram_links(text)
        assert len(matches) == 1
        assert matches[0].scheme_type == "tg_privatepost"
        assert matches[0].channel_ref == "1707179235"
        assert matches[0].message_id == 11808

    def test_extract_public_channel_link(self):
        text = 'Follow https://t.me/my_source_channel/11808 for news'
        matches = extract_internal_telegram_links(text)
        assert len(matches) == 1
        assert matches[0].channel_ref == "my_source_channel"
        assert matches[0].message_id == 11808

    def test_extract_tg_resolve(self):
        text = 'tg://resolve?domain=my_source_channel&post=11808'
        matches = extract_internal_telegram_links(text)
        assert len(matches) == 1
        assert matches[0].scheme_type == "tg_resolve"
        assert matches[0].channel_ref == "my_source_channel"
        assert matches[0].message_id == 11808

    def test_ignores_non_telegram_links(self):
        text = 'Visit https://google.com/search or https://example.com/c/123/456'
        matches = extract_internal_telegram_links(text)
        assert len(matches) == 0


class TestFormatDestinationUrl:
    def test_format_private_dest_url(self):
        url = format_destination_url(
            dest_chat_id=-1002674892914,
            sent_message_id=5432,
            extra="?single",
        )
        assert url == "https://t.me/c/2674892914/5432?single"

    def test_format_public_dest_url(self):
        url = format_destination_url(
            dest_chat_id=-1002674892914,
            sent_message_id=5432,
            dest_username="dest_channel_name",
        )
        assert url == "https://t.me/dest_channel_name/5432"

    def test_format_tg_privatepost(self):
        url = format_destination_url(
            dest_chat_id=-1002674892914,
            sent_message_id=5432,
            scheme_type="tg_privatepost",
        )
        assert url == "tg://privatepost?channel=2674892914&post=5432"


class TestRewriting:
    def test_rewrite_text_links_in_html(self):
        text = '<a href="https://t.me/c/1707179235/11808">follow</a>'
        link_map = {"https://t.me/c/1707179235/11808": "https://t.me/c/2674892914/5432"}
        new_text, changed = rewrite_text_links(text, link_map)
        assert changed is True
        assert new_text == '<a href="https://t.me/c/2674892914/5432">follow</a>'

    def test_rewrite_reply_markup_links(self):
        markup = {
            "inline_keyboard": [
                [{"text": "Previous", "url": "https://t.me/c/1707179235/11808"}],
                [{"text": "External", "url": "https://google.com"}],
            ]
        }
        link_map = {"https://t.me/c/1707179235/11808": "https://t.me/c/2674892914/5432"}
        new_markup, changed = rewrite_reply_markup_links(markup, link_map)
        assert changed is True
        assert new_markup["inline_keyboard"][0][0]["url"] == "https://t.me/c/2674892914/5432"
        assert new_markup["inline_keyboard"][1][0]["url"] == "https://google.com"


@pytest.mark.asyncio
class TestDatabaseAndPayloadRewriting:
    @pytest.fixture
    def db_path(self, tmp_path):
        return str(tmp_path / "test.db")

    async def _init_db(self, db):
        db.row_factory = aiosqlite.Row
        await db.execute(
            """
            CREATE TABLE message_reply_map (
                source_chat_id INTEGER NOT NULL,
                source_message_id INTEGER NOT NULL,
                destination_chat_id INTEGER NOT NULL,
                sent_message_id INTEGER NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (source_chat_id, source_message_id, destination_chat_id)
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_message_id INTEGER NOT NULL,
                source_chat_id INTEGER NOT NULL,
                status TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE message_destinations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL,
                destination_chat_id INTEGER NOT NULL,
                sent_message_id INTEGER,
                status TEXT
            )
            """
        )
        # Insert mapping for source msg 11808 in destination -1002674892914 -> 5432
        await db.execute(
            """
            INSERT INTO message_reply_map (source_chat_id, source_message_id, destination_chat_id, sent_message_id)
            VALUES (?, ?, ?, ?)
            """,
            (-1001707179235, 11808, -1002674892914, 5432),
        )
        # Insert mapping for destination -1005555555555 -> 9999
        await db.execute(
            """
            INSERT INTO message_reply_map (source_chat_id, source_message_id, destination_chat_id, sent_message_id)
            VALUES (?, ?, ?, ?)
            """,
            (-1001707179235, 11808, -1005555555555, 9999),
        )
        await db.commit()

    async def test_lookup_sent_message_id_from_reply_map(self, db_path):
        async with aiosqlite.connect(db_path) as db:
            await self._init_db(db)
            sent_id = await lookup_sent_message_id(
                db,
                source_chat_ids=[-1001707179235, 1707179235],
                source_message_id=11808,
                dest_chat_id=-1002674892914,
            )
            assert sent_id == 5432

    async def test_lookup_sent_message_id_unmapped_returns_none(self, db_path):
        async with aiosqlite.connect(db_path) as db:
            await self._init_db(db)
            sent_id = await lookup_sent_message_id(
                db,
                source_chat_ids=[-1001707179235],
                source_message_id=99999,
                dest_chat_id=-1002674892914,
            )
            assert sent_id is None

    async def test_resolve_destination_link_map(self, db_path):
        async with aiosqlite.connect(db_path) as db:
            await self._init_db(db)
            texts = ['See <a href="https://t.me/c/1707179235/11808">follow</a>']
            link_map = await resolve_destination_link_map(
                db=db,
                source_chat_id=-1001707179235,
                dest_chat_id=-1002674892914,
                texts=texts,
            )
            assert "https://t.me/c/1707179235/11808" in link_map
            assert link_map["https://t.me/c/1707179235/11808"] == "https://t.me/c/2674892914/5432"

    async def test_rewrite_payload_for_destination(self, db_path):
        async with aiosqlite.connect(db_path) as db:
            await self._init_db(db)
            payload = {
                "type": "text",
                "message_id": 11850,
                "chat_id": -1001707179235,
                "text": "See follow guide",
                "text_html": 'See <a href="https://t.me/c/1707179235/11808">follow</a> guide',
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "Go to post", "url": "https://t.me/c/1707179235/11808"}]
                    ]
                },
            }
            processed_payload = {
                **payload,
                "text_changed": False,
                "destinations": [{"chat_id": -1002674892914, "name": "Dest A"}],
            }

            # Test for Destination A
            dest_payload_a = await rewrite_payload_for_destination(
                payload=payload,
                processed_payload=processed_payload,
                db=db,
                source_chat_id=-1001707179235,
                dest_chat_id=-1002674892914,
            )

            assert dest_payload_a["text_changed"] is True
            assert 'href="https://t.me/c/2674892914/5432"' in dest_payload_a["processed_text_html"]
            assert dest_payload_a["reply_markup"]["inline_keyboard"][0][0]["url"] == "https://t.me/c/2674892914/5432"

            # Test for Destination B (different message ID mapped)
            dest_payload_b = await rewrite_payload_for_destination(
                payload=payload,
                processed_payload=processed_payload,
                db=db,
                source_chat_id=-1001707179235,
                dest_chat_id=-1005555555555,
            )

            assert dest_payload_b["text_changed"] is True
            assert 'href="https://t.me/c/5555555555/9999"' in dest_payload_b["processed_text_html"]
            assert dest_payload_b["reply_markup"]["inline_keyboard"][0][0]["url"] == "https://t.me/c/5555555555/9999"

    async def test_rewrite_payload_album_captions(self, db_path):
        async with aiosqlite.connect(db_path) as db:
            await self._init_db(db)
            payload = {
                "type": "album",
                "message_id": 11850,
                "chat_id": -1001707179235,
                "items": [
                    {
                        "message_id": 11850,
                        "caption": "Photo 1",
                        "caption_html": '<a href="https://t.me/c/1707179235/11808">Photo 1 link</a>',
                    },
                    {
                        "message_id": 11851,
                        "caption": "Photo 2 plain",
                        "caption_html": None,
                    },
                ],
            }
            processed_payload = {
                **payload,
                "any_caption_changed": False,
                "destinations": [{"chat_id": -1002674892914, "name": "Dest A"}],
            }

            dest_payload = await rewrite_payload_for_destination(
                payload=payload,
                processed_payload=processed_payload,
                db=db,
                source_chat_id=-1001707179235,
                dest_chat_id=-1002674892914,
            )

            assert dest_payload["any_caption_changed"] is True
            item0 = dest_payload["items"][0]
            assert item0["caption_changed"] is True
            assert 'href="https://t.me/c/2674892914/5432"' in item0["processed_caption_html"]
            assert dest_payload["items"][1].get("caption_changed", False) is False

