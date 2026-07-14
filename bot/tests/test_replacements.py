"""Tests for word replacement logic (mirrors n8n, implemented in replacements.py)."""

import pytest

from replacements import apply_replacements


class TestWordReplacement:

    def test_simple_string_replacement(self):
        rules = [{"pattern": "old_word", "replacement": "new_word", "is_regex": False}]
        assert apply_replacements("hello old_word", rules) == "hello new_word"

    def test_multiple_occurrences(self):
        rules = [{"pattern": "foo", "replacement": "bar", "is_regex": False}]
        assert apply_replacements("foo and foo", rules) == "bar and bar"

    def test_channel_replacement(self):
        rules = [{"pattern": "@old_channel", "replacement": "@new_channel", "is_regex": False}]
        result = apply_replacements("Follow @old_channel for updates", rules)
        assert result == "Follow @new_channel for updates"

    def test_url_regex_replacement(self):
        rules = [{
            "pattern": r"https?://original\.com(/\S*)?",
            "replacement": r"https://new-domain.com\1",
            "is_regex": True,
        }]
        result = apply_replacements("Visit https://original.com/page", rules)
        assert result == "Visit https://new-domain.com/page"

    def test_unicode_replacement(self):
        rules = [{"pattern": "привет", "replacement": "здравствуйте", "is_regex": False}]
        assert apply_replacements("привет мир", rules) == "здравствуйте мир"

    def test_emoji_in_text(self):
        rules = [{"pattern": "hello", "replacement": "hi", "is_regex": False}]
        assert apply_replacements("hello 🌍 world", rules) == "hi 🌍 world"

    def test_no_rules(self):
        assert apply_replacements("unchanged text", []) == "unchanged text"

    def test_none_text(self):
        assert apply_replacements(None, [{"pattern": "foo", "replacement": "bar"}]) is None
