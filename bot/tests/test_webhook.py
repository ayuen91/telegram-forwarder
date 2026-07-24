"""Tests for HMAC webhook signature generation and verification."""

import hashlib
import hmac
import json
import pytest

from webhook import _canonical_json, _strip_nulls


def generate_signature(payload: dict, secret: str) -> str:
    """Replicate the WebhookSender's signing logic."""
    payload_json = _canonical_json(payload)
    return hmac.new(
        secret.encode("utf-8"),
        payload_json.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def verify_signature(payload_json: str, signature: str, secret: str) -> bool:
    """Replicate what n8n does to verify."""
    expected = hmac.new(
        secret.encode("utf-8"),
        payload_json.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(signature, expected)


def stable_stringify(value):
    """Match n8n stableStringify on null-stripped payloads."""
    import json
    value = _strip_nulls(value)
    if value is None or not isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + ",".join(stable_stringify(item) for item in value) + "]"
    keys = sorted(value.keys())
    parts = [json.dumps(k, ensure_ascii=False) + ":" + stable_stringify(value[k]) for k in keys]
    return "{" + ",".join(parts) + "}"


class TestWebhookSignature:
    SECRET = "test-secret-key-12345"

    def test_signature_generation(self):
        """Signature should be a 64-char hex string."""
        payload = {"message_id": 1, "text": "hello"}
        sig = generate_signature(payload, self.SECRET)
        assert len(sig) == 64
        assert all(c in "0123456789abcdef" for c in sig)

    def test_signature_verification(self):
        """Generated signature should pass verification."""
        payload = {"message_id": 1, "text": "hello world"}
        payload_json = _canonical_json(payload)
        sig = generate_signature(payload, self.SECRET)
        assert verify_signature(payload_json, sig, self.SECRET) is True

    def test_wrong_secret_fails(self):
        """Signature with wrong secret should fail verification."""
        payload = {"message_id": 1, "text": "hello"}
        payload_json = json.dumps(payload, default=str, separators=(",", ":"), sort_keys=True)
        sig = generate_signature(payload, self.SECRET)
        assert verify_signature(payload_json, sig, "wrong-secret") is False

    def test_tampered_payload_fails(self):
        """Modified payload should fail signature verification."""
        payload = {"message_id": 1, "text": "hello"}
        sig = generate_signature(payload, self.SECRET)

        tampered = {"message_id": 1, "text": "hacked"}
        tampered_json = json.dumps(tampered, default=str)
        assert verify_signature(tampered_json, sig, self.SECRET) is False

    def test_different_payloads_different_sigs(self):
        """Different payloads should produce different signatures."""
        sig1 = generate_signature({"id": 1}, self.SECRET)
        sig2 = generate_signature({"id": 2}, self.SECRET)
        assert sig1 != sig2

    def test_same_payload_same_sig(self):
        """Same payload should always produce same signature (deterministic)."""
        payload = {"message_id": 1, "text": "test", "type": "text"}
        sig1 = generate_signature(payload, self.SECRET)
        sig2 = generate_signature(payload, self.SECRET)
        assert sig1 == sig2

    def test_unicode_payload(self):
        """Signature should handle unicode text correctly."""
        payload = {"message_id": 1, "text": "Привет мир 🌍"}
        sig = generate_signature(payload, self.SECRET)
        payload_json = _canonical_json(payload)
        assert verify_signature(payload_json, sig, self.SECRET) is True

    def test_photo_payload_null_fields_stripped(self):
        """Photo payloads omit null text/caption — must match n8n body after parse."""
        payload = {
            "message_id": 40,
            "chat_id": -100123,
            "type": "photo",
            "text": None,
            "caption": None,
            "entities": [],
            "caption_entities": [],
            "has_media": True,
            "reply_to_message_id": None,
            "timestamp": "2026-07-14T11:51:10+00:00",
        }
        py_json = _canonical_json(payload)
        js_style = stable_stringify(payload)
        assert py_json == js_style
        sig = generate_signature(payload, self.SECRET)
        assert verify_signature(py_json, sig, self.SECRET) is True

    def test_stable_stringify_matches_python_dumps(self):
        """n8n compact stable stringify must match Python signing."""
        payload = {
            "message_id": 1,
            "text": "hello",
            "destinations": [{"chat_id": -100, "name": "A", "enabled": True}],
        }
        py_json = _canonical_json(payload)
        js_style = stable_stringify(payload)
        assert py_json == js_style
        sig = generate_signature(payload, self.SECRET)
        assert verify_signature(py_json, sig, self.SECRET) is True
        assert verify_signature(js_style, sig, self.SECRET) is True

    def test_album_64bit_media_group_id_signature(self):
        """Album payload with 64-bit media_group_id string must maintain precision in HMAC."""
        payload = {
            "type": "album",
            "media_group_id": "14279032192983933",
            "chat_id": -1001774783341,
            "item_count": 2,
            "items": [
                {
                    "message_id": 29964,
                    "chat_id": -1001774783341,
                    "type": "photo",
                    "caption": "Test caption",
                    "media_group_id": "14279032192983933",
                    "reply_to_message_id": 29960,
                },
                {
                    "message_id": 29965,
                    "chat_id": -1001774783341,
                    "type": "photo",
                    "media_group_id": "14279032192983933",
                    "reply_to_message_id": 29960,
                },
            ],
            "reply_to_message_id": 29960,
        }
        py_json = _canonical_json(payload)
        js_style = stable_stringify(payload)
        assert py_json == js_style
        sig = generate_signature(payload, self.SECRET)
        assert verify_signature(py_json, sig, self.SECRET) is True
        assert verify_signature(js_style, sig, self.SECRET) is True


