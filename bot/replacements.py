"""Word replacement engine — mirrors n8n logic, runs in Python."""

import re
from typing import Any, Dict, List, Optional, Tuple


def _shift_entities(
    entities: List[Dict[str, Any]],
    replace_start: int,
    old_len: int,
    new_len: int,
) -> List[Dict[str, Any]]:
    """
    Adjust entity offsets/lengths after a single in-place text substitution.

    Rules (mirrors Telegram's own entity-shift semantics):
    - Entities that end before the replaced span → untouched.
    - Entities that start after the replaced span → shift offset by delta.
    - Entities that fully enclose the replaced span → grow/shrink by delta.
    - Entities that *overlap* the replaced span are dropped (their bounds
      are now undefined after the substitution).
    """
    replace_end = replace_start + old_len
    delta = new_len - old_len
    result: List[Dict[str, Any]] = []
    for ent in entities:
        e_start = ent["offset"]
        e_end = e_start + ent["length"]

        if e_end <= replace_start:
            # Entity ends before replaced region — untouched
            result.append(ent)
        elif e_start >= replace_end:
            # Entity starts after replaced region — shift
            shifted = dict(ent)
            shifted["offset"] = e_start + delta
            result.append(shifted)
        elif e_start <= replace_start and e_end >= replace_end:
            # Entity fully encloses the replacement — expand/shrink length
            enclosing = dict(ent)
            enclosing["length"] = ent["length"] + delta
            if enclosing["length"] > 0:
                result.append(enclosing)
        # else: entity overlaps the replaced region — drop it
    return result


def apply_replacements(
    text: Optional[str],
    rules: List[Dict[str, Any]],
    entities: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[Optional[str], Optional[List[Dict[str, Any]]]]:
    """
    Apply word replacement rules to *text*, returning (new_text, adjusted_entities).

    Entity offsets are shifted to match each plain-text substitution so that
    formatting (bold, italic, underline, mono, links, etc.) is preserved after
    replacement.  When a *regex* rule fires, entities are set to None because
    arbitrary regex substitutions make offset tracking unpredictable.

    Returns (text, entities) — both may be the original values if no rule matched.
    """
    if not text or not rules:
        return text, entities

    result = text
    live_entities: Optional[List[Dict[str, Any]]] = list(entities) if entities else []
    entities_valid = True  # flipped False on first regex hit

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
            if new_result != result:
                result = new_result
                # Regex changes make offset tracking impossible — drop entities
                entities_valid = False
                live_entities = []
        else:
            # Plain-text replacement: find every occurrence and shift entities
            old_len = len(pattern)
            new_len = len(replacement)
            search_start = 0
            while True:
                idx = result.find(pattern, search_start)
                if idx == -1:
                    break
                result = result[:idx] + replacement + result[idx + old_len:]
                if entities_valid and live_entities:
                    live_entities = _shift_entities(live_entities, idx, old_len, new_len)
                search_start = idx + new_len

    final_entities = live_entities if (entities_valid and live_entities) else None
    return result, final_entities


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
                orig_entities = item_copy.get("caption_entities") or []
                processed, adj_entities = apply_replacements(caption, rules, orig_entities)
                item_copy["processed_caption"] = processed
                # Carry adjusted entities so the sender can re-apply them
                if adj_entities is not None:
                    item_copy["processed_caption_entities"] = adj_entities
                elif processed != caption:
                    # Regex replacement fired — entities are invalid, clear them
                    item_copy["processed_caption_entities"] = []
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
        orig_entities = payload.get("entities") or []
        processed_text, adj_entities = apply_replacements(text, rules, orig_entities)
        result["processed_text"] = processed_text
        result["text_changed"] = processed_text != text
        # Always store adjusted entities (None means use originals; [] means dropped)
        if adj_entities is not None:
            result["processed_entities"] = adj_entities
        elif processed_text != text:
            result["processed_entities"] = []

    if caption:
        orig_caption_entities = payload.get("caption_entities") or []
        processed_caption, adj_caption_entities = apply_replacements(
            caption, rules, orig_caption_entities
        )
        result["processed_caption"] = processed_caption
        result["caption_changed"] = processed_caption != caption
        if adj_caption_entities is not None:
            result["processed_caption_entities"] = adj_caption_entities
        elif processed_caption != caption:
            result["processed_caption_entities"] = []

    result["destinations"] = destinations
    return result


def needs_n8n(payload: Dict[str, Any]) -> bool:
    """Only route through n8n when there is text/caption to edit."""
    if payload.get("use_native_forward"):
        return False
    if payload.get("type") == "album":
        return any(item.get("caption") for item in payload.get("items", []))
    return bool(payload.get("text") or payload.get("caption"))
