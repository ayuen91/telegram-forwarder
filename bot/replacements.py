"""Word replacement engine — applies replacements locally, no external service required."""

import json
import re
from typing import Any, Dict, List, Optional, Tuple


def _strip_html_tags(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text)


def _extract_href_urls(text: str) -> List[str]:
    """Extract all URLs from href attributes in HTML text."""
    if not text or "<" not in text:
        return []
    return re.findall(r'href\s*=\s*["\']([^"\']+)["\']', text, flags=re.IGNORECASE)


def _rule_matches_text(text: str, rule: Dict[str, Any]) -> bool:
    pattern = rule.get("pattern")
    if not pattern or not text:
        return False
    if rule.get("is_regex"):
        try:
            return re.search(pattern, text, flags=re.MULTILINE) is not None
        except re.error:
            return False
    return pattern in text


def _any_rule_matches(
    texts: List[str],
    rules: List[Dict[str, Any]],
    reply_markup: Optional[Dict[str, Any]] = None,
) -> bool:
    """Return True if any replacement rule would match any text, HTML href, or button."""
    if not rules:
        return False

    for text in texts:
        if not text:
            continue
        plain = _strip_html_tags(text) if "<" in text else text
        hrefs = _extract_href_urls(text)

        for rule in rules:
            # Check text matches if replace_text is enabled
            if rule.get("replace_text", True) and _rule_matches_text(plain, rule):
                return True
            # Check hyperlink/href matches if replace_url is enabled
            if rule.get("replace_url", False):
                for href in hrefs:
                    if _rule_matches_text(href, rule):
                        return True

    if reply_markup and isinstance(reply_markup, dict):
        for row in reply_markup.get("inline_keyboard", []):
            for btn in row:
                btn_text = btn.get("text", "")
                btn_url = btn.get("url") or (btn.get("web_app", {}) or {}).get("url") or (btn.get("login_url", {}) or {}).get("url") or ""
                for rule in rules:
                    if rule.get("replace_text", True) and btn_text and _rule_matches_text(btn_text, rule):
                        return True
                    if rule.get("replace_url", False) and btn_url and _rule_matches_text(btn_url, rule):
                        return True

    return False


def _collect_replaceable_texts(payload: Dict[str, Any]) -> List[str]:
    """Plain text / caption / quote fields that word replacement may alter."""
    texts: List[str] = []
    if payload.get("type") == "album":
        for item in payload.get("items", []):
            cap = item.get("caption_html") or item.get("caption")
            if cap:
                texts.append(cap)
            q_item = item.get("reply_to_quote_html") or item.get("reply_to_quote_text")
            if q_item:
                texts.append(q_item)
        q_album = payload.get("reply_to_quote_html") or payload.get("reply_to_quote_text")
        if q_album:
            texts.append(q_album)
        return texts

    for key in ("text_html", "text", "caption_html", "caption", "reply_to_quote_html", "reply_to_quote_text"):
        val = payload.get(key)
        if val:
            texts.append(val)
    return texts


def _replace_href_in_tag(tag: str, rule: Dict[str, Any]) -> str:
    """Replace URL inside href='...' or href=\"...\" attribute of an HTML tag."""
    if not rule.get("replace_url", False):
        return tag
    pattern = rule.get("pattern")
    if not pattern:
        return tag
    replacement = rule.get("replacement", "")

    def _sub_href(m):
        prefix, url, suffix = m.group(1), m.group(2), m.group(3)
        if rule.get("is_regex"):
            try:
                new_url = re.sub(pattern, replacement, url, flags=re.MULTILINE)
            except re.error:
                new_url = url
        else:
            new_url = url.replace(pattern, replacement)
        return f"{prefix}{new_url}{suffix}"

    return re.sub(r'(href\s*=\s*["\'])([^"\']+)(["\'])', _sub_href, tag, flags=re.IGNORECASE)


def apply_replacements(
    text: Optional[str],
    rules: List[Dict[str, Any]],
) -> Tuple[Optional[str], bool]:
    """
    Apply word and hyperlink replacement rules to *text*.

    Safely handles both plain text and HTML-formatted text (text_html / caption_html).
    Splits content by HTML tags:
      - Rules with replace_url=True apply to href="..." attributes inside HTML tags.
      - Rules with replace_text=True apply to text content between HTML tags.

    Returns (new_text, changed) where changed is True if any rule fired.
    """
    if not text or not rules:
        return text, False

    # Split text into HTML tags and non-tag text tokens
    tokens = re.split(r"(<[^>]+>)", text)
    result_tokens = []

    for token in tokens:
        if token.startswith("<") and token.endswith(">"):
            # HTML tag — apply replace_url rules to href attribute
            tag = token
            for rule in rules:
                if rule.get("replace_url", False):
                    tag = _replace_href_in_tag(tag, rule)
            result_tokens.append(tag)
        else:
            # Text content between tags — apply replace_text rules
            chunk = token
            for rule in rules:
                if not rule.get("replace_text", True):
                    continue
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


def _apply_quote_replacements(
    payload: Dict[str, Any],
    rules: List[Dict[str, Any]],
    result: Dict[str, Any],
) -> None:
    quote_html_src = payload.get("reply_to_quote_html")
    quote_plain_src = payload.get("reply_to_quote_text")
    if quote_html_src:
        processed_html_quote, changed = apply_replacements(quote_html_src, rules)
        result["processed_reply_to_quote_html"] = processed_html_quote
        result["processed_reply_to_quote"] = re.sub(r"<[^>]+>", "", processed_html_quote)
        result["reply_to_quote_changed"] = changed
    elif quote_plain_src:
        processed_plain_quote, changed = apply_replacements(quote_plain_src, rules)
        result["processed_reply_to_quote"] = processed_plain_quote
        result["reply_to_quote_changed"] = changed
    else:
        result["reply_to_quote_changed"] = False


def _apply_reply_markup_replacements(
    reply_markup: Optional[Dict[str, Any]],
    rules: List[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], bool]:
    """Apply replacement rules to inline keyboard button labels and URLs."""
    if not reply_markup or not rules:
        return reply_markup, False

    new_markup = json.loads(json.dumps(reply_markup))
    changed = False

    for row in new_markup.get("inline_keyboard", []):
        for btn in row:
            # Button text replacement (replace_text rules)
            if btn.get("text"):
                new_btn_text, btn_text_changed = apply_replacements(
                    btn["text"],
                    [r for r in rules if r.get("replace_text", True)],
                )
                if btn_text_changed:
                    btn["text"] = new_btn_text
                    changed = True

            # Button URL replacement (replace_url rules)
            if btn.get("url"):
                url_val = btn["url"]
                for rule in rules:
                    if rule.get("replace_url", False):
                        pat = rule.get("pattern")
                        rep = rule.get("replacement", "")
                        if pat:
                            if rule.get("is_regex"):
                                try:
                                    url_val = re.sub(pat, rep, url_val, flags=re.MULTILINE)
                                except re.error:
                                    pass
                            else:
                                url_val = url_val.replace(pat, rep)
                if url_val != btn["url"]:
                    btn["url"] = url_val
                    changed = True

            # WebApp URL replacement
            if btn.get("web_app", {}) and btn["web_app"].get("url"):
                wa_url = btn["web_app"]["url"]
                for rule in rules:
                    if rule.get("replace_url", False):
                        pat = rule.get("pattern")
                        rep = rule.get("replacement", "")
                        if pat:
                            if rule.get("is_regex"):
                                try:
                                    wa_url = re.sub(pat, rep, wa_url, flags=re.MULTILINE)
                                except re.error:
                                    pass
                            else:
                                wa_url = wa_url.replace(pat, rep)
                if wa_url != btn["web_app"]["url"]:
                    btn["web_app"]["url"] = wa_url
                    changed = True

            # Login URL replacement
            if btn.get("login_url", {}) and btn["login_url"].get("url"):
                lg_url = btn["login_url"]["url"]
                for rule in rules:
                    if rule.get("replace_url", False):
                        pat = rule.get("pattern")
                        rep = rule.get("replacement", "")
                        if pat:
                            if rule.get("is_regex"):
                                try:
                                    lg_url = re.sub(pat, rep, lg_url, flags=re.MULTILINE)
                                except re.error:
                                    pass
                            else:
                                lg_url = lg_url.replace(pat, rep)
                if lg_url != btn["login_url"]["url"]:
                    btn["login_url"]["url"] = lg_url
                    changed = True

    return (new_markup if changed else reply_markup), changed


def build_processed_payload(payload: Dict[str, Any], config) -> Dict[str, Any]:
    """Apply replacements locally and attach destinations (no n8n required)."""
    destinations = [
        {
            "chat_id": d.chat_id,
            "name": d.name,
            "enabled": d.enabled,
            "username": getattr(d, "username", ""),
        }
        for d in config.get_active_destinations()
    ]
    source_username = getattr(config.settings, "source_username", "") if hasattr(config, "settings") else ""

    # Native forward path preserves attribution — skip word replacements
    if payload.get("use_native_forward"):
        result = dict(payload)
        result["destinations"] = destinations
        if source_username:
            result["source_username"] = source_username
        return result

    rules = [
        {
            "pattern": r.pattern,
            "replacement": r.replacement,
            "is_regex": r.is_regex,
            "replace_text": getattr(r, "replace_text", True),
            "replace_url": getattr(r, "replace_url", False),
        }
        for r in config.settings.replacement_rules
    ]

    # Skip replacement entirely when no rule matches text/caption/URLs/buttons — preserves
    # native copyMessage delivery and avoids touching quote metadata.
    if not _any_rule_matches(
        _collect_replaceable_texts(payload),
        rules,
        payload.get("reply_markup"),
    ):
        result = dict(payload)
        result["text_changed"] = False
        result["caption_changed"] = False
        result["reply_to_quote_changed"] = False
        if payload.get("type") == "album":
            items = []
            for item in payload.get("items", []):
                item_copy = dict(item)
                item_copy["caption_changed"] = False
                items.append(item_copy)
            result["items"] = items
            result["any_caption_changed"] = False
        if source_username:
            result["source_username"] = source_username
        result["destinations"] = destinations
        return result

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
                item_copy["caption_changed"] = changed
                if changed:
                    any_caption_changed = True
            items.append(item_copy)
        result = {
            **payload,
            "items": items,
            "any_caption_changed": any_caption_changed,
            "destinations": destinations,
        }
        if source_username:
            result["source_username"] = source_username
        _apply_quote_replacements(payload, rules, result)
        if payload.get("reply_markup"):
            new_markup, markup_changed = _apply_reply_markup_replacements(payload["reply_markup"], rules)
            if markup_changed:
                result["reply_markup"] = new_markup
        return result

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

    _apply_quote_replacements(payload, rules, result)

    if payload.get("reply_markup"):
        new_markup, markup_changed = _apply_reply_markup_replacements(payload["reply_markup"], rules)
        if markup_changed:
            result["reply_markup"] = new_markup

    if source_username:
        result["source_username"] = source_username
    result["destinations"] = destinations
    return result
