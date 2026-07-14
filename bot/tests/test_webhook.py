"""Tests for HMAC webhook signature generation and verification."""

import hashlib
import hmac
import json
import pytest


def generate_signature(payload: dict, secret: str) -> str:
    """Replicate the WebhookSender's signing logic."""
    payload_json = json.dumps(payload, default=str, separators=(",", ":"))
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
        payload_json = json.dumps(payload, default=str, separators=(",", ":"))
        sig = generate_signature(payload, self.SECRET)
        assert verify_signature(payload_json, sig, self.SECRET) is True

    def test_wrong_secret_fails(self):
        """Signature with wrong secret should fail verification."""
        payload = {"message_id": 1, "text": "hello"}
        payload_json = json.dumps(payload, default=str, separators=(",", ":"))
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
        payload_json = json.dumps(payload, default=str, separators=(",", ":"))
        assert verify_signature(payload_json, sig, self.SECRET) is True

    def test_empty_payload(self):
        """Signature should work with empty dict."""
        payload = {}
        sig = generate_signature(payload, self.SECRET)
        payload_json = json.dumps(payload, default=str, separators=(",", ":"))
        assert verify_signature(payload_json, sig, self.SECRET) is True
