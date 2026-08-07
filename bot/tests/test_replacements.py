"""Tests for word replacement logic (mirrors n8n, implemented in replacements.py)."""

import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock

from replacements import apply_replacements, build_processed_payload, _any_rule_matches


class TestWordReplacement:

    def test_simple_string_replacement(self):
        rules = [{"pattern": "old_word", "replacement": "new_word", "is_regex": False}]
        text, changed = apply_replacements("hello old_word", rules)
        assert text == "hello new_word"
        assert changed is True

    def test_multiple_occurrences(self):
        rules = [{"pattern": "foo", "replacement": "bar", "is_regex": False}]
        text, changed = apply_replacements("foo and foo", rules)
        assert text == "bar and bar"
        assert changed is True

    def test_channel_replacement(self):
        rules = [{"pattern": "@old_channel", "replacement": "@new_channel", "is_regex": False}]
        text, changed = apply_replacements("Follow @old_channel for updates", rules)
        assert text == "Follow @new_channel for updates"
        assert changed is True

    def test_url_regex_replacement(self):
        rules = [{
            "pattern": r"https?://original\.com(/\S*)?",
            "replacement": r"https://new-domain.com\1",
            "is_regex": True,
        }]
        text, changed = apply_replacements("Visit https://original.com/page", rules)
        assert text == "Visit https://new-domain.com/page"
        assert changed is True

    def test_unicode_replacement(self):
        rules = [{"pattern": "привет", "replacement": "здравствуйте", "is_regex": False}]
        text, changed = apply_replacements("привет мир", rules)
        assert text == "здравствуйте мир"
        assert changed is True

    def test_emoji_in_text(self):
        rules = [{"pattern": "hello", "replacement": "hi", "is_regex": False}]
        text, changed = apply_replacements("hello 🌍 world", rules)
        assert text == "hi 🌍 world"
        assert changed is True

    def test_no_rules(self):
        text, changed = apply_replacements("unchanged text", [])
        assert text == "unchanged text"
        assert changed is False

    def test_none_text(self):
        text, changed = apply_replacements(None, [{"pattern": "foo", "replacement": "bar"}])
        assert text is None
        assert changed is False

    def test_html_tags_preserved_when_no_match(self):
        rules = [{"pattern": "missing", "replacement": "x", "is_regex": False}]
        html = "<b>hello</b> world"
        text, changed = apply_replacements(html, rules)
        assert text == html
        assert changed is False

    def test_html_replacement_only_in_text_nodes(self):
        rules = [{"pattern": "world", "replacement": "earth", "is_regex": False}]
        html = "<b>hello</b> world"
        text, changed = apply_replacements(html, rules)
        assert text == "<b>hello</b> earth"
        assert changed is True


class TestBuildProcessedPayload:
    def _config(self, rules):
        config = MagicMock()
        dest = SimpleNamespace(chat_id=-1001, name="Dest", enabled=True)
        config.get_active_destinations.return_value = [dest]
        config.settings.replacement_rules = [
            SimpleNamespace(pattern=r["pattern"], replacement=r["replacement"], is_regex=r.get("is_regex", False))
            for r in rules
        ]
        return config

    def test_skips_replacement_when_no_rule_matches(self):
        config = self._config([{"pattern": "nope", "replacement": "x"}])
        payload = {
            "type": "text",
            "text": "hello world",
            "text_html": "<b>hello</b> world",
            "message_id": 1,
            "chat_id": -100,
        }
        result = build_processed_payload(payload, config)
        assert result["text_changed"] is False
        assert "processed_text" not in result
        assert result["text_html"] == "<b>hello</b> world"

    def test_applies_replacement_when_rule_matches(self):
        config = self._config([{"pattern": "world", "replacement": "earth"}])
        payload = {
            "type": "text",
            "text": "hello world",
            "text_html": "<b>hello</b> world",
            "message_id": 1,
            "chat_id": -100,
        }
        result = build_processed_payload(payload, config)
        assert result["text_changed"] is True
        assert result["processed_text_html"] == "<b>hello</b> earth"

    def test_album_skips_when_no_caption_match(self):
        config = self._config([{"pattern": "nope", "replacement": "x"}])
        payload = {
            "type": "album",
            "items": [{"message_id": 1, "caption": "cap", "caption_html": "<b>cap</b>"}],
            "chat_id": -100,
        }
        result = build_processed_payload(payload, config)
        assert result["any_caption_changed"] is False
        assert result["items"][0]["caption_changed"] is False

    def test_any_rule_matches_plain_and_html(self):
        rules = [{"pattern": "foo", "replacement": "bar", "is_regex": False}]
        assert _any_rule_matches(["<b>foo</b> baz"], rules) is True
        assert _any_rule_matches(["<b>bar</b> baz"], rules) is False
