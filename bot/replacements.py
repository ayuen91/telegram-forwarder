"""Word replacement engine — mirrors n8n logic, runs in Python."""

import re
from typing import Any, Dict, List, Optional, Tuple


def apply_replacements(
    text: Optional[str],
    rules: List[Dict[str, Any]],
) -> Tuple[Optional[str], bool]:
    """
    Apply word replacement rules to *text*.

    Returns (new_text, changed) where changed is True if any rule fired.
    Formatting is now carried as HTML strings (text_html / caption_html) so
    entity-offset tracking is no longer required here.
    """
    if not text or not rules:
        return text, False

    result = text

    for rule in rules:
        pattern = rule.get("pattern")
        if not pattern:
            continue
        replacement = rule.get("replacement", "")

        if rule.get("is_regex"):
            try:
                new_result = re.sub(pattern, replacement, result, flags=re.MULTILINE)
            except re.error:
                continue
            result = new_result
        else:
            result = result.replace(pattern, replacement)

    return result, result != text


def build_processed_payload(payload: Dict[str, Any], config) -> Dict[str, Any]:
    """Apply replacements locally and attach destinations (no n8n required)."""
    destinations = [
        {"chat_id": d.chat_id, "name": d.name, "enabled": d.enabled}
        for d in config.get_active_destinations()
    ]

    # Native forward path preserves attribution — skip word replacements
    if payload.get("use_native_forward"):
        result = dict(payload)
        result["destinations"] = destinations
        return result

    rules = [
        {"pattern": r.pattern, "replacement": r.replacement, "is_regex": r.is_regex}
        for r in config.settings.replacement_rules
    ]

    if payload.get("type") == "album":
        items = []
        any_caption_changed = False
        for item in payload.get("items", []):
            item_copy = dict(item)
            caption = item_copy.get("caption")
            if caption:
                processed, changed = apply_replacements(caption, rules)
                item_copy["processed_caption"] = processed
                if changed:
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
        processed_text, changed = apply_replacements(text, rules)
        result["processed_text"] = processed_text
        result["text_changed"] = changed

    if caption:
        processed_caption, changed = apply_replacements(caption, rules)
        result["processed_caption"] = processed_caption
        result["caption_changed"] = changed

    result["destinations"] = destinations
    return result


def needs_n8n(payload: Dict[str, Any]) -> bool:
    """Only route through n8n when there is text/caption to edit."""
    if payload.get("use_native_forward"):
        return False
    if payload.get("type") == "album":
        return any(item.get("caption") for item in payload.get("items", []))
    return bool(payload.get("text") or payload.get("caption"))
