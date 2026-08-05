"""Word replacement engine — applies replacements locally, no external service required."""

import re
from typing import Any, Dict, List, Optional, Tuple



def apply_replacements(
    text: Optional[str],
    rules: List[Dict[str, Any]],
) -> Tuple[Optional[str], bool]:
    """
    Apply word replacement rules to *text*.

    Safely handles both plain text and HTML-formatted text (text_html / caption_html).
    Splits content by HTML tags so replacements apply only to text between tags,
    preserving all HTML entities, bold/italic markup, links, and custom emoji tags.

    Returns (new_text, changed) where changed is True if any rule fired.
    """
    if not text or not rules:
        return text, False

    # Split text into HTML tags and non-tag text tokens
    tokens = re.split(r"(<[^>]+>)", text)
    result_tokens = []

    for token in tokens:
        if token.startswith("<") and token.endswith(">"):
            # HTML tag — preserve as-is
            result_tokens.append(token)
        else:
            # Text content between tags — apply replacement rules
            chunk = token
            for rule in rules:
                pattern = rule.get("pattern")
                if not pattern:
                    continue
                replacement = rule.get("replacement", "")

                if rule.get("is_regex"):
                    try:
                        chunk = re.sub(pattern, replacement, chunk, flags=re.MULTILINE)
                    except re.error:
                        continue
                else:
                    chunk = chunk.replace(pattern, replacement)
            result_tokens.append(chunk)

    new_text = "".join(result_tokens)
    return new_text, new_text != text


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
            caption_src = item_copy.get("caption_html") or item_copy.get("caption")
            if caption_src:
                processed, changed = apply_replacements(caption_src, rules)
                item_copy["processed_caption"] = processed
                if item_copy.get("caption_html"):
                    item_copy["processed_caption_html"] = processed
                item_copy["caption_changed"] = changed  # authoritative per-item flag
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
    text_src = payload.get("text_html") or payload.get("text")
    caption_src = payload.get("caption_html") or payload.get("caption")

    if text_src:
        processed_text, changed = apply_replacements(text_src, rules)
        result["processed_text"] = processed_text
        if payload.get("text_html"):
            result["processed_text_html"] = processed_text
        result["text_changed"] = changed
    else:
        result["text_changed"] = False

    if caption_src:
        processed_caption, changed = apply_replacements(caption_src, rules)
        result["processed_caption"] = processed_caption
        if payload.get("caption_html"):
            result["processed_caption_html"] = processed_caption
        result["caption_changed"] = changed
    else:
        result["caption_changed"] = False

    # Apply replacements to the quoted text independently for HTML and plain variants.
    # processed_reply_to_quote must always be plain text (no HTML tags) so that the
    # Bot API can do a plain-text substring match against the destination message copy.
    quote_html_src = payload.get("reply_to_quote_html")
    quote_plain_src = payload.get("reply_to_quote_text")
    if quote_html_src:
        processed_html_quote, changed = apply_replacements(quote_html_src, rules)
        result["processed_reply_to_quote_html"] = processed_html_quote
        # Strip all HTML tags to produce a clean plain-text version for Bot API matching.
        result["processed_reply_to_quote"] = re.sub(r"<[^>]+>", "", processed_html_quote)
        result["reply_to_quote_changed"] = changed
    elif quote_plain_src:
        processed_plain_quote, changed = apply_replacements(quote_plain_src, rules)
        result["processed_reply_to_quote"] = processed_plain_quote
        result["reply_to_quote_changed"] = changed
    else:
        result["reply_to_quote_changed"] = False

    result["destinations"] = destinations
    return result


