"""
Internal Telegram message hyperlink mapping and rewriting engine.

Detects internal Telegram message links pointing to the source channel
(e.g., https://t.me/c/1707179235/11808, t.me/c/1707179235/11808, tg://privatepost, etc.)
and rewrites them to point to the corresponding forwarded message in each destination channel
(e.g., https://t.me/c/2674892914/5432).
"""

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple, Union

logger = logging.getLogger(__name__)

# ── Regex patterns for Telegram message URLs ──────────────────────────────────────────

# Private post URL: t.me/c/<channel_id>/[<thread_id>/]<msg_id>[?<query>][#<hash>]
RE_PRIVATE_POST = re.compile(
    r"""(?P<full_url>(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/c/(?P<channel>\d+)(?:/(?P<thread>\d+))?/(?P<msg_id>\d+)(?P<extra>(?:\?[^"'\s<>]*|#[^"'\s<>]*)?))""",
    re.IGNORECASE,
)

# tg://privatepost?channel=<channel_id>&post=<msg_id>
RE_TG_PRIVATEPOST = re.compile(
    r"""(?P<full_url>tg://privatepost\?channel=(?P<channel>\d+)&(?:amp;)?post=(?P<msg_id>\d+)(?P<extra>[^"'\s<>]*))""",
    re.IGNORECASE,
)

# Public post URL: t.me/<username>/[<thread_id>/]<msg_id>[?<query>][#<hash>]
# Exclude standard Telegram system prefixes that are not channel usernames
RE_PUBLIC_POST = re.compile(
    r"""(?P<full_url>(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/(?!(?:c|joinchat|addstickers|addtheme|addemoji|share|invoice|login|s|proxy|socks)/)(?P<username>[a-zA-Z0-9_]{4,32})(?:/(?P<thread>\d+))?/(?P<msg_id>\d+)(?P<extra>(?:\?[^"'\s<>]*|#[^"'\s<>]*)?))""",
    re.IGNORECASE,
)

# tg://resolve?domain=<username>&post=<msg_id>
RE_TG_RESOLVE = re.compile(
    r"""(?P<full_url>tg://resolve\?domain=(?P<username>[a-zA-Z0-9_]{4,32})&(?:amp;)?post=(?P<msg_id>\d+)(?P<extra>[^"'\s<>]*))""",
    re.IGNORECASE,
)


@dataclass
class InternalLinkMatch:
    raw_url: str
    channel_ref: str  # Numeric string (e.g. '1707179235') or username ('channel_name')
    message_id: int
    thread_id: Optional[int] = None
    extra: str = ""
    scheme_type: str = "t.me"  # 't.me', 'tg_privatepost', 'tg_resolve'


def normalize_channel_id_for_url(chat_id: Union[int, str]) -> str:
    """
    Normalize a Telegram chat ID to the numeric string format used in t.me/c/ URLs.

    Examples:
        -1001707179235 -> '1707179235'
        '-1001707179235' -> '1707179235'
        -1707179235 -> '1707179235'
        1707179235 -> '1707179235'
    """
    s = str(chat_id).strip()
    if s.startswith("-100"):
        return s[4:]
    if s.startswith("-"):
        return s[1:]
    if s.startswith("100") and len(s) > 12:
        return s[3:]
    return s


def channel_id_candidates(chat_id: Union[int, str]) -> List[int]:
    """
    Generate candidate integer chat IDs for database lookups from a chat ID or URL channel token.

    Example:
        '1707179235' -> [-1001707179235, -1707179235, 1707179235]
    """
    candidates: List[int] = []
    try:
        raw_int = int(str(chat_id).strip())
        norm_str = normalize_channel_id_for_url(raw_int)
        norm_int = int(norm_str)
        candidates.append(int(f"-100{norm_int}"))
        candidates.append(-norm_int)
        candidates.append(norm_int)
    except (ValueError, TypeError):
        pass
    return list(dict.fromkeys(candidates))


def _clean_extra(extra: Optional[str]) -> str:
    """Clean trailing sentence punctuation from URL query/hash fragments."""
    if not extra:
        return ""
    cleaned = re.sub(r"[.,;:!?)]+$", "", extra)
    return cleaned


def extract_internal_telegram_links(text: Optional[str]) -> List[InternalLinkMatch]:
    """
    Extract all Telegram message deep links from text or HTML attributes.
    """
    if not text:
        return []

    matches: List[InternalLinkMatch] = []
    seen_urls: Set[str] = set()

    # 1. Private post links (t.me/c/<channel>/<msg_id>)
    for m in RE_PRIVATE_POST.finditer(text):
        full_url = m.group("full_url")
        extra = _clean_extra(m.group("extra"))
        if extra != (m.group("extra") or ""):
            full_url = full_url[: len(full_url) - (len(m.group("extra") or "") - len(extra))]
        if full_url not in seen_urls:
            seen_urls.add(full_url)
            thread = int(m.group("thread")) if m.group("thread") else None
            matches.append(
                InternalLinkMatch(
                    raw_url=full_url,
                    channel_ref=m.group("channel"),
                    message_id=int(m.group("msg_id")),
                    thread_id=thread,
                    extra=extra,
                    scheme_type="t.me",
                )
            )

    # 2. tg://privatepost?channel=<channel>&post=<msg_id>
    for m in RE_TG_PRIVATEPOST.finditer(text):
        full_url = m.group("full_url")
        extra = _clean_extra(m.group("extra"))
        if extra != (m.group("extra") or ""):
            full_url = full_url[: len(full_url) - (len(m.group("extra") or "") - len(extra))]
        if full_url not in seen_urls:
            seen_urls.add(full_url)
            matches.append(
                InternalLinkMatch(
                    raw_url=full_url,
                    channel_ref=m.group("channel"),
                    message_id=int(m.group("msg_id")),
                    extra=extra,
                    scheme_type="tg_privatepost",
                )
            )

    # 3. Public post links (t.me/<username>/<msg_id>)
    for m in RE_PUBLIC_POST.finditer(text):
        full_url = m.group("full_url")
        extra = _clean_extra(m.group("extra"))
        if extra != (m.group("extra") or ""):
            full_url = full_url[: len(full_url) - (len(m.group("extra") or "") - len(extra))]
        if full_url not in seen_urls:
            seen_urls.add(full_url)
            thread = int(m.group("thread")) if m.group("thread") else None
            matches.append(
                InternalLinkMatch(
                    raw_url=full_url,
                    channel_ref=m.group("username"),
                    message_id=int(m.group("msg_id")),
                    thread_id=thread,
                    extra=extra,
                    scheme_type="t.me",
                )
            )

    # 4. tg://resolve?domain=<username>&post=<msg_id>
    for m in RE_TG_RESOLVE.finditer(text):
        full_url = m.group("full_url")
        extra = _clean_extra(m.group("extra"))
        if extra != (m.group("extra") or ""):
            full_url = full_url[: len(full_url) - (len(m.group("extra") or "") - len(extra))]
        if full_url not in seen_urls:
            seen_urls.add(full_url)
            matches.append(
                InternalLinkMatch(
                    raw_url=full_url,
                    channel_ref=m.group("username"),
                    message_id=int(m.group("msg_id")),
                    extra=extra,
                    scheme_type="tg_resolve",
                )
            )

    return matches


def format_destination_url(
    dest_chat_id: int,
    sent_message_id: int,
    dest_username: Optional[str] = None,
    extra: str = "",
    scheme_type: str = "t.me",
) -> str:
    """
    Construct a destination Telegram URL for a mapped message.

    If a username is provided for the destination, generates:
        https://t.me/<username>/<sent_message_id>[<extra>]
    Otherwise generates private channel deep link:
        https://t.me/c/<dest_channel_id>/<sent_message_id>[<extra>]
    """
    clean_username = dest_username.lstrip("@").strip() if dest_username else ""
    if clean_username:
        if scheme_type == "tg_resolve":
            return f"tg://resolve?domain={clean_username}&post={sent_message_id}{extra}"
        return f"https://t.me/{clean_username}/{sent_message_id}{extra}"

    dest_url_channel = normalize_channel_id_for_url(dest_chat_id)
    if scheme_type == "tg_privatepost":
        return f"tg://privatepost?channel={dest_url_channel}&post={sent_message_id}{extra}"
    return f"https://t.me/c/{dest_url_channel}/{sent_message_id}{extra}"


async def lookup_sent_message_id(
    db,
    source_chat_ids: List[int],
    source_message_id: int,
    dest_chat_id: int,
) -> Optional[int]:
    """
    Query database for the forwarded sent_message_id in dest_chat_id.
    Checks message_reply_map first, then messages + message_destinations.
    """
    if not source_chat_ids:
        return None

    placeholders = ",".join("?" for _ in source_chat_ids)

    # 1. Check message_reply_map
    cursor = await db.execute(
        f"""
        SELECT sent_message_id FROM message_reply_map
        WHERE source_chat_id IN ({placeholders})
          AND source_message_id = ?
          AND destination_chat_id = ?
        LIMIT 1
        """,
        (*source_chat_ids, int(source_message_id), int(dest_chat_id)),
    )
    row = await cursor.fetchone()
    if row and row["sent_message_id"]:
        return int(row["sent_message_id"])

    # 2. Fall back to messages + message_destinations table
    cursor = await db.execute(
        f"""
        SELECT md.sent_message_id
        FROM messages m
        JOIN message_destinations md ON m.id = md.message_id
        WHERE m.source_chat_id IN ({placeholders})
          AND m.telegram_message_id = ?
          AND md.destination_chat_id = ?
          AND md.status = 'sent'
          AND md.sent_message_id IS NOT NULL
        LIMIT 1
        """,
        (*source_chat_ids, int(source_message_id), int(dest_chat_id)),
    )
    row = await cursor.fetchone()
    if row and row["sent_message_id"]:
        return int(row["sent_message_id"])

    return None


def rewrite_text_links(
    text: Optional[str],
    link_map: Dict[str, str],
) -> Tuple[Optional[str], bool]:
    """
    Replace mapped URLs in text or HTML attributes.
    Returns (new_text, changed).
    """
    if not text or not link_map:
        return text, False

    result = text
    # Replace longest URLs first to avoid substring partial replacement collisions
    for old_url, new_url in sorted(link_map.items(), key=lambda x: len(x[0]), reverse=True):
        if old_url in result:
            result = result.replace(old_url, new_url)

    return result, result != text


def rewrite_reply_markup_links(
    reply_markup: Optional[Dict[str, Any]],
    link_map: Dict[str, str],
) -> Tuple[Optional[Dict[str, Any]], bool]:
    """
    Recursively rewrite URLs inside inline keyboard buttons.
    """
    if not reply_markup or not link_map:
        return reply_markup, False

    changed = False
    new_markup = json.loads(json.dumps(reply_markup))
    inline_keyboard = new_markup.get("inline_keyboard", [])

    for row in inline_keyboard:
        for btn in row:
            if "url" in btn and btn["url"]:
                old_btn_url = btn["url"]
                new_btn_url, btn_changed = rewrite_text_links(old_btn_url, link_map)
                if btn_changed:
                    btn["url"] = new_btn_url
                    changed = True

    return (new_markup if changed else reply_markup), changed


async def resolve_destination_link_map(
    db,
    source_chat_id: int,
    dest_chat_id: int,
    texts: List[str],
    dest_username: Optional[str] = None,
    source_username: Optional[str] = None,
) -> Dict[str, str]:
    """
    Scan all texts for internal Telegram links, query the database for mappings in dest_chat_id,
    and return an old_url -> new_url dictionary.
    """
    source_norm = normalize_channel_id_for_url(source_chat_id)
    source_candidates = channel_id_candidates(source_chat_id)
    clean_src_username = source_username.lstrip("@").lower() if source_username else ""

    all_matches: List[InternalLinkMatch] = []
    for t in texts:
        if t:
            all_matches.extend(extract_internal_telegram_links(t))

    link_map: Dict[str, str] = {}

    for match in all_matches:
        is_match = False
        target_candidates = source_candidates

        # Check if match is numeric channel ID matching source channel
        if match.channel_ref.isdigit():
            match_norm = normalize_channel_id_for_url(match.channel_ref)
            if match_norm == source_norm:
                is_match = True
            else:
                # Also check if it matches candidate IDs
                match_candidates = channel_id_candidates(match.channel_ref)
                if any(c in source_candidates for c in match_candidates):
                    is_match = True
                else:
                    # Could be another mapped channel
                    target_candidates = match_candidates
                    is_match = True
        else:
            # Username match
            match_user = match.channel_ref.lstrip("@").lower()
            if clean_src_username and match_user == clean_src_username:
                is_match = True

        if not is_match:
            continue

        sent_msg_id = await lookup_sent_message_id(
            db,
            target_candidates,
            match.message_id,
            dest_chat_id,
        )

        if sent_msg_id is not None:
            new_url = format_destination_url(
                dest_chat_id=dest_chat_id,
                sent_message_id=sent_msg_id,
                dest_username=dest_username,
                extra=match.extra,
                scheme_type=match.scheme_type,
            )
            link_map[match.raw_url] = new_url
            logger.info(
                f"[LINK_REWRITE] Mapped internal link {match.raw_url} -> {new_url} "
                f"for dest {dest_chat_id} (source msg={match.message_id} -> sent msg={sent_msg_id})"
            )

    return link_map


async def rewrite_payload_for_destination(
    payload: Dict[str, Any],
    processed_payload: Dict[str, Any],
    db,
    source_chat_id: int,
    dest_chat_id: int,
    dest_username: Optional[str] = None,
    source_username: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a destination-specific copy of processed_payload with all internal hyperlinks rewritten.
    """
    # 1. Collect all texts and button URLs across payload & processed_payload
    texts: List[str] = []

    for key in (
        "processed_text_html",
        "processed_text",
        "text_html",
        "text",
        "processed_caption_html",
        "processed_caption",
        "caption_html",
        "caption",
        "processed_reply_to_quote_html",
        "processed_reply_to_quote",
        "reply_to_quote_html",
        "reply_to_quote_text",
    ):
        v = processed_payload.get(key) or payload.get(key)
        if v and isinstance(v, str):
            texts.append(v)

    # Album items
    items = processed_payload.get("items") or payload.get("items") or []
    for item in items:
        for k in ("processed_caption_html", "processed_caption", "caption_html", "caption"):
            iv = item.get(k)
            if iv and isinstance(iv, str):
                texts.append(iv)

    # Reply markup button URLs
    reply_markup = processed_payload.get("reply_markup") or payload.get("reply_markup")
    if reply_markup and isinstance(reply_markup, dict):
        for row in reply_markup.get("inline_keyboard", []):
            for btn in row:
                if btn.get("url"):
                    texts.append(btn["url"])

    # 2. Resolve link map for this destination
    link_map = await resolve_destination_link_map(
        db=db,
        source_chat_id=source_chat_id,
        dest_chat_id=dest_chat_id,
        texts=texts,
        dest_username=dest_username,
        source_username=source_username,
    )

    if not link_map:
        return processed_payload

    # 3. Apply link map to create destination-specific processed_payload copy
    dest_payload = dict(processed_payload)

    # Text rewriting
    text_src_html = dest_payload.get("processed_text_html") or payload.get("text_html")
    text_src_plain = dest_payload.get("processed_text") or payload.get("text")
    if text_src_html:
        new_text_html, changed = rewrite_text_links(text_src_html, link_map)
        if changed:
            dest_payload["processed_text_html"] = new_text_html
            dest_payload["processed_text"] = re.sub(r"<[^>]+>", "", new_text_html)
            dest_payload["text_changed"] = True
    elif text_src_plain:
        new_text_plain, changed = rewrite_text_links(text_src_plain, link_map)
        if changed:
            dest_payload["processed_text"] = new_text_plain
            dest_payload["processed_text_html"] = new_text_plain
            dest_payload["text_changed"] = True

    # Caption rewriting
    cap_src_html = dest_payload.get("processed_caption_html") or payload.get("caption_html")
    cap_src_plain = dest_payload.get("processed_caption") or payload.get("caption")
    if cap_src_html:
        new_cap_html, changed = rewrite_text_links(cap_src_html, link_map)
        if changed:
            dest_payload["processed_caption_html"] = new_cap_html
            dest_payload["processed_caption"] = re.sub(r"<[^>]+>", "", new_cap_html)
            dest_payload["caption_changed"] = True
    elif cap_src_plain:
        new_cap_plain, changed = rewrite_text_links(cap_src_plain, link_map)
        if changed:
            dest_payload["processed_caption"] = new_cap_plain
            dest_payload["processed_caption_html"] = new_cap_plain
            dest_payload["caption_changed"] = True

    # Album items rewriting
    if items:
        new_items = []
        any_caption_changed = dest_payload.get("any_caption_changed", False)
        for item in items:
            item_copy = dict(item)
            item_html = item_copy.get("processed_caption_html") or item_copy.get("caption_html")
            item_plain = item_copy.get("processed_caption") or item_copy.get("caption")
            if item_html:
                new_item_html, changed = rewrite_text_links(item_html, link_map)
                if changed:
                    item_copy["processed_caption_html"] = new_item_html
                    item_copy["processed_caption"] = re.sub(r"<[^>]+>", "", new_item_html)
                    item_copy["caption_changed"] = True
                    any_caption_changed = True
            elif item_plain:
                new_item_plain, changed = rewrite_text_links(item_plain, link_map)
                if changed:
                    item_copy["processed_caption"] = new_item_plain
                    item_copy["processed_caption_html"] = new_item_plain
                    item_copy["caption_changed"] = True
                    any_caption_changed = True
            new_items.append(item_copy)
        dest_payload["items"] = new_items
        dest_payload["any_caption_changed"] = any_caption_changed

    # Reply quote rewriting
    quote_src_html = dest_payload.get("processed_reply_to_quote_html") or payload.get("reply_to_quote_html")
    quote_src_plain = dest_payload.get("processed_reply_to_quote") or payload.get("reply_to_quote_text")
    if quote_src_html:
        new_q_html, changed = rewrite_text_links(quote_src_html, link_map)
        if changed:
            dest_payload["processed_reply_to_quote_html"] = new_q_html
            dest_payload["processed_reply_to_quote"] = re.sub(r"<[^>]+>", "", new_q_html)
            dest_payload["reply_to_quote_changed"] = True
    elif quote_src_plain:
        new_q_plain, changed = rewrite_text_links(quote_src_plain, link_map)
        if changed:
            dest_payload["processed_reply_to_quote"] = new_q_plain
            dest_payload["reply_to_quote_changed"] = True

    # Reply markup (inline keyboard) rewriting
    if reply_markup:
        new_markup, markup_changed = rewrite_reply_markup_links(reply_markup, link_map)
        if markup_changed:
            dest_payload["reply_markup"] = new_markup

    return dest_payload
