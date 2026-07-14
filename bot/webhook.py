"""
HMAC-signed webhook sender to n8n.

Signs every payload with HMAC-SHA256 using a shared secret. n8n verifies
the signature before processing. n8n returns the processed payload (with
word replacements applied) as JSON, which we parse and return to the caller.

Two endpoints:
  - /webhook/message — single messages
  - /webhook/album   — album payloads
"""

import copy
import hashlib
import hmac
import json
import logging
from typing import Dict, Any, Optional

import aiohttp

logger = logging.getLogger(__name__)


def _strip_nulls(value: Any) -> Any:
    """Remove None fields so signed JSON matches n8n's parsed webhook body."""
    if isinstance(value, dict):
        return {k: _strip_nulls(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_strip_nulls(item) for item in value]
    return value


def _canonical_json(payload: Dict[str, Any]) -> str:
    """Stable JSON matching n8n stableStringify (sorted keys, no nulls, compact)."""
    cleaned = _strip_nulls(payload)
    return json.dumps(cleaned, default=str, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


class WebhookSender:
    """Send HMAC-signed payloads to n8n webhook endpoints."""

    def __init__(
        self,
        message_url: str,
        album_url: str,
        secret: str,
        config=None,
        connect_timeout: float = 5.0,
        read_timeout: float = 30.0,
    ):
        self.message_url = message_url
        self.album_url = album_url
        self.secret = secret
        self._config = config  # Config instance for injecting destinations/rules
        self.timeout = aiohttp.ClientTimeout(
            connect=connect_timeout,
            total=read_timeout + connect_timeout,
        )
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazy-initialize aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        return self._session

    def _sign_payload(self, payload_json: str) -> str:
        """Generate HMAC-SHA256 signature for a JSON payload string."""
        return hmac.new(
            self.secret.encode("utf-8"),
            payload_json.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def send(
        self, payload: Dict[str, Any], endpoint: str = "message"
    ) -> Optional[Dict[str, Any]]:
        """
        Send a signed payload to n8n and return the processed result.

        n8n applies word replacements and returns the enriched payload as JSON.

        Args:
            payload: Message or album data dict
            endpoint: "message" or "album"

        Returns:
            Parsed response dict from n8n if successful, None otherwise.
        """
        url = self.album_url if endpoint == "album" else self.message_url

        outbound = copy.deepcopy(payload)
        if self._config:
            s = self._config.settings
            outbound["destinations"] = [
                {"chat_id": d.chat_id, "name": d.name, "enabled": d.enabled}
                for d in s.destinations if d.enabled
            ]
            outbound["replacement_rules"] = [
                {"pattern": r.pattern, "replacement": r.replacement, "is_regex": r.is_regex}
                for r in s.replacement_rules
            ]

        payload_json = _canonical_json(outbound)
        signature = self._sign_payload(payload_json)

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "X-Signature": signature,
        }

        message_id = payload.get("message_id", payload.get("media_group_id", "unknown"))

        try:
            session = await self._get_session()
            async with session.post(url, data=payload_json.encode("utf-8"), headers=headers) as resp:
                body_text = await resp.text()
                if 200 <= resp.status < 300:
                    if not body_text or not body_text.strip():
                        logger.warning(
                            f"Webhook empty response: {endpoint} message_id={message_id} "
                            f"(likely n8n HMAC rejection)"
                        )
                        return None
                    try:
                        result = json.loads(body_text)
                        if not result.get("destinations"):
                            logger.warning(
                                f"Webhook missing destinations: {endpoint} message_id={message_id}"
                            )
                            return None
                        logger.info(
                            f"Webhook sent: {endpoint} message_id={message_id} status={resp.status}"
                        )
                        return result
                    except json.JSONDecodeError:
                        logger.warning(
                            f"Webhook non-JSON response: {endpoint} message_id={message_id} "
                            f"body={body_text[:200]}"
                        )
                        return None
                else:
                    logger.warning(
                        f"Webhook failed: {endpoint} message_id={message_id} "
                        f"status={resp.status} body={body_text[:200]}"
                    )
                    return None

        except aiohttp.ClientConnectorError as e:
            logger.warning(f"Webhook connection failed ({endpoint}): {e}")
            return None
        except aiohttp.ClientError as e:
            logger.warning(f"Webhook error ({endpoint}): {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected webhook error ({endpoint}): {e}", exc_info=True)
            return None

    async def is_reachable(self) -> bool:
        """
        Check if n8n webhook endpoint is reachable (HEAD request).
        Used by health checks.
        """
        try:
            session = await self._get_session()
            async with session.head(self.message_url) as resp:
                # n8n may return 404 for HEAD on webhook, but connection works
                return resp.status < 500
        except Exception:
            return False

    async def close(self):
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
