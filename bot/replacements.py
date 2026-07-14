"""Word replacement engine — mirrors n8n logic, runs in Python."""

import re
from typing import Any, Dict, List, Optional


def apply_replacements(text: Optional[str], rules: List[Dict[str, Any]]) -> Optional[str]:
    if not text or not rules:
        return text

    result = text
    for rule in rules:
        pattern = rule.get("pattern")
        if not pattern:
            continue
        replacement = rule.get("replacement", "")
        if rule.get("is_regex"):
            try:
                result = re.sub(pattern, replacement, result, flags=re.MULTILINE)
            except re.error:
                continue
        else:
            result = result.replace(pattern, replacement)
    return result


def build_processed_payload(payload: Dict[str, Any], config) -> Dict[str, Any]:
    """Apply replacements locally and attach destinations (no n8n required)."""
    rules = [
        {"pattern": r.pattern, "replacement": r.replacement, "is_regex": r.is_regex}
        for r in config.settings.replacement_rules
    ]
    destinations = [
        {"chat_id": d.chat_id, "name": d.name, "enabled": d.enabled}
        for d in config.get_active_destinations()
    ]

    if payload.get("type") == "album":
        items = []
        any_caption_changed = False
        for item in payload.get("items", []):
            item_copy = dict(item)
            caption = item_copy.get("caption")
            if caption:
                processed = apply_replacements(caption, rules)
                item_copy["processed_caption"] = processed
                if processed != caption:
                    any_caption_changed = True
            items.append(item_copy)
        return {
            **payload,
            "items": items,
            "any_caption_changed": any_caption_changed,
            "destinations": destinations,
        }

    result = dict(payload)
    text = payload.get("text")
    caption = payload.get("caption")

    if text:
        processed_text = apply_replacements(text, rules)
        result["processed_text"] = processed_text
        result["text_changed"] = processed_text != text

    if caption:
        processed_caption = apply_replacements(caption, rules)
        result["processed_caption"] = processed_caption
        result["caption_changed"] = processed_caption != caption

    result["destinations"] = destinations
    return result


def needs_n8n(payload: Dict[str, Any]) -> bool:
    """Only route through n8n when there is text/caption to edit."""
    if payload.get("type") == "album":
        return any(item.get("caption") for item in payload.get("items", []))
    return bool(payload.get("text") or payload.get("caption"))
