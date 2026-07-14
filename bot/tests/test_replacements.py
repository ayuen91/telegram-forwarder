"""Tests for word replacement logic (mirrors what n8n's Function node does)."""

import re
import pytest


def apply_replacements(text: str, rules: list) -> str:
    """
    Apply word replacement rules to text.
    Mirrors the n8n Function node logic for testing purposes.
    """
    if not text:
        return text

    result = text
    for rule in rules:
        pattern = rule["pattern"]
        replacement = rule.get("replacement", "")
        is_regex = rule.get("is_regex", False)

        if is_regex:
            result = re.sub(pattern, replacement, result)
        else:
            result = result.replace(pattern, replacement)

    return result


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

    def test_url_without_path(self):
        rules = [{
            "pattern": r"https?://original\.com(/\S*)?",
            "replacement": r"https://new-domain.com\1",
            "is_regex": True,
        }]
        result = apply_replacements("Visit https://original.com", rules)
        assert result == "Visit https://new-domain.com"

    def test_remove_text(self):
        rules = [{"pattern": r"Sponsored by.*$", "replacement": "", "is_regex": True}]
        result = apply_replacements("Great content Sponsored by Company", rules)
        assert result == "Great content "

    def test_case_insensitive_regex(self):
        rules = [{"pattern": "(?i)old brand", "replacement": "New Brand", "is_regex": True}]
        assert apply_replacements("check OLD BRAND", rules) == "check New Brand"
        assert apply_replacements("check Old Brand", rules) == "check New Brand"

    def test_multiple_rules_applied_in_order(self):
        rules = [
            {"pattern": "alpha", "replacement": "beta", "is_regex": False},
            {"pattern": "beta", "replacement": "gamma", "is_regex": False},
        ]
        # alpha -> beta -> gamma (rules applied in order)
        assert apply_replacements("alpha", rules) == "gamma"

    def test_empty_text(self):
        rules = [{"pattern": "foo", "replacement": "bar", "is_regex": False}]
        assert apply_replacements("", rules) == ""

    def test_none_text(self):
        rules = [{"pattern": "foo", "replacement": "bar", "is_regex": False}]
        assert apply_replacements(None, rules) is None

    def test_no_rules(self):
        assert apply_replacements("unchanged text", []) == "unchanged text"

    def test_no_match(self):
        rules = [{"pattern": "missing", "replacement": "found", "is_regex": False}]
        assert apply_replacements("nothing here", rules) == "nothing here"

    def test_unicode_replacement(self):
        rules = [{"pattern": "привет", "replacement": "здравствуйте", "is_regex": False}]
        assert apply_replacements("привет мир", rules) == "здравствуйте мир"

    def test_emoji_in_text(self):
        rules = [{"pattern": "hello", "replacement": "hi", "is_regex": False}]
        assert apply_replacements("hello 🌍 world", rules) == "hi 🌍 world"

    def test_caption_and_text_both_processed(self):
        """Simulate processing both text and caption fields."""
        rules = [{"pattern": "@old", "replacement": "@new", "is_regex": False}]
        text_result = apply_replacements("Follow @old", rules)
        caption_result = apply_replacements("Photo by @old", rules)
        assert text_result == "Follow @new"
        assert caption_result == "Photo by @new"
