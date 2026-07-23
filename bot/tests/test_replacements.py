"""Tests for word replacement logic (mirrors n8n, implemented in replacements.py)."""

import pytest

from replacements import apply_replacements


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
