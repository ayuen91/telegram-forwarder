-- Telegram Forwarder Database Schema
-- Applied automatically on first bot startup by main.py

-- Enable WAL mode for better concurrent read/write performance
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- Messages table: tracks every message through the pipeline
CREATE TABLE IF NOT EXISTS messages (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_message_id INTEGER NOT NULL,
    source_chat_id      INTEGER NOT NULL,
    message_type        TEXT NOT NULL,      -- 'text','photo','video','document','sticker','voice','animation','audio','video_note','album','poll','contact','location','venue'
    original_text       TEXT,               -- For text messages
    original_caption    TEXT,               -- For media messages
    processed_text      TEXT,               -- After word replacement
    processed_caption   TEXT,               -- After word replacement
    has_media           INTEGER DEFAULT 0,  -- 0/1 (SQLite has no BOOLEAN)
    media_group_id      TEXT,               -- Non-null for album items
    album_item_count    INTEGER,            -- Number of items in album (for album type)
    status              TEXT NOT NULL DEFAULT 'received',
    -- Status flow: received -> queued -> processing -> sent | failed | dead_letter
    error_message       TEXT,
    retry_count         INTEGER DEFAULT 0,
    received_at         TEXT DEFAULT (datetime('now')),
    processed_at        TEXT,
    sent_at             TEXT,
    UNIQUE(telegram_message_id, source_chat_id)
);

-- Destination tracking: one message can go to multiple destinations
CREATE TABLE IF NOT EXISTS message_destinations (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id          INTEGER NOT NULL REFERENCES messages(id),
    destination_chat_id INTEGER NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    -- Status: pending -> sent | failed
    sent_message_id     INTEGER,           -- The message_id in the destination chat
    sent_at             TEXT,
    error_message       TEXT,
    retry_count         INTEGER DEFAULT 0
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status);
CREATE INDEX IF NOT EXISTS idx_messages_telegram_id ON messages(telegram_message_id, source_chat_id);
CREATE INDEX IF NOT EXISTS idx_messages_received ON messages(received_at);
CREATE INDEX IF NOT EXISTS idx_dest_status ON message_destinations(status);
CREATE INDEX IF NOT EXISTS idx_dest_message ON message_destinations(message_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dest_message_chat
    ON message_destinations(message_id, destination_chat_id);

-- Reply threading: maps each source message_id to its sent id per destination.
-- Needed for channel replies and for replies to any item in an album.
CREATE TABLE IF NOT EXISTS message_reply_map (
    source_chat_id      INTEGER NOT NULL,
    source_message_id   INTEGER NOT NULL,
    destination_chat_id INTEGER NOT NULL,
    sent_message_id     INTEGER NOT NULL,
    created_at          TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (source_chat_id, source_message_id, destination_chat_id)
);

CREATE INDEX IF NOT EXISTS idx_reply_map_lookup
    ON message_reply_map(source_chat_id, source_message_id, destination_chat_id);
