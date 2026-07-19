"""
Configuration loader with hot-reload support.

Loads settings from .env (environment variables) and YAML config files
(channels.yml, replacements.yml). YAML files are hot-reloaded every health
check cycle by comparing file modification times — no restart needed.
"""

import os
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any, Optional

import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load .env file (only on import — env vars don't hot-reload)
load_dotenv()


def _require_env(key: str) -> str:
    """Get required environment variable or fail fast."""
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return value


def _env_int(key: str, default: int) -> int:
    return int(os.getenv(key, str(default)))


def _env_float(key: str, default: float) -> float:
    return float(os.getenv(key, str(default)))


def _env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass
class Destination:
    chat_id: int
    name: str
    enabled: bool = True


@dataclass
class ReplacementRule:
    pattern: str
    replacement: str
    is_regex: bool = False


@dataclass
class OriginChannel:
    chat_id: int
    name: str = ""


@dataclass
class ForwardAttributionSettings:
    """When enabled, preserve 'Forwarded from' via Bot API forwardMessage."""

    enabled: bool = True
    allowed_origin_channels: List[OriginChannel] = field(default_factory=list)


@dataclass
class Settings:
    """Application settings — immutable env vars + hot-reloadable YAML."""

    # Telegram User Bot (from .env)
    api_id: int = 0
    api_hash: str = ""
    phone_number: str = ""
    source_chat_id: int = 0

    # Telegram Sender Bot (Bot API — destination delivery only)
    bot_token: str = ""
    relay_channel_id: int = 0  # Private channel for media relay (recommended)

    # n8n Webhooks (from .env)
    n8n_webhook_message: str = ""
    n8n_webhook_album: str = ""
    webhook_secret: str = ""

    # Redis (from .env)
    redis_url: str = ""

    # SQLite (from .env)
    db_path: str = ""

    # Alert Bot (from .env)
    alert_bot_token: str = ""
    alert_chat_id: int = 0

    # Tuning (from .env)
    worker_count: int = 2
    worker_delay_min: float = 0.5
    worker_delay_max: float = 1.5
    album_buffer_seconds: float = 2.0
    health_check_interval: int = 300
    max_retries: int = 3
    log_level: str = "INFO"

    # Listener tuning (from .env)
    # silence_timeout: seconds of no push messages before PTS audit + client recycle (0 = disabled)
    # The watchdog first calls updates.GetState() to confirm the silence is a real stall
    # (server PTS > local snapshot) before performing a full stop()→start() recycle.
    listener_silence_timeout: int = 900      # 15 minutes — triggers PTS audit + client recycle

    # Daily health report (from .env)
    daily_report_enabled: bool = True
    daily_report_hour: int = 8
    daily_report_timezone: str = "Etc/GMT-3"  # fixed UTC+3
    daily_report_run_on_start: bool = False

    # Hot-reloadable (from YAML)
    destinations: List[Destination] = field(default_factory=list)
    replacement_rules: List[ReplacementRule] = field(default_factory=list)
    forward_attribution: ForwardAttributionSettings = field(
        default_factory=ForwardAttributionSettings
    )


class Config:
    """
    Manages application configuration with hot-reload for YAML files.

    Usage:
        config = Config()
        settings = config.settings

        # In health check cycle:
        config.check_and_reload()
    """

    # Default to /app/config — the Docker volume mount point.
    # Can be overridden via CONFIG_DIR env var for non-Docker setups.
    CONFIG_DIR = os.getenv("CONFIG_DIR", "/app/config")
    CHANNELS_PATH = os.path.join(CONFIG_DIR, "channels.yml")
    REPLACEMENTS_PATH = os.path.join(CONFIG_DIR, "replacements.yml")

    def __init__(self):
        self._channels_mtime: float = 0
        self._replacements_mtime: float = 0
        self.settings = Settings()
        self._load_env()
        self._load_channels()
        self._load_replacements()

    def _load_env(self):
        """Load settings from environment variables. Called once at startup."""
        s = self.settings

        s.api_id = _env_int("API_ID", 0)
        s.api_hash = _require_env("API_HASH")
        s.phone_number = _require_env("PHONE_NUMBER")
        s.bot_token = _require_env("BOT_TOKEN")

        # Source chat ID — loaded from channels.yml, but can be overridden via env
        # Will be set in _load_channels if not in env
        source_env = os.getenv("SOURCE_CHAT_ID")
        if source_env:
            s.source_chat_id = int(source_env)

        s.n8n_webhook_message = _require_env("N8N_WEBHOOK_URL_MESSAGE")
        s.n8n_webhook_album = _require_env("N8N_WEBHOOK_URL_ALBUM")
        s.webhook_secret = _require_env("WEBHOOK_SECRET")

        s.redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
        s.db_path = os.getenv("DB_PATH", "/app/data/forwarder.db")

        s.alert_bot_token = _require_env("ALERT_BOT_TOKEN")
        s.alert_chat_id = _env_int("ALERT_CHAT_ID", 0)

        relay_env = os.getenv("RELAY_CHANNEL_ID") or os.getenv("RELAY_CHAT_ID")
        if relay_env:
            s.relay_channel_id = int(relay_env)

        s.worker_count = _env_int("WORKER_COUNT", 2)
        s.worker_delay_min = _env_float("WORKER_DELAY_MIN", 0.5)
        s.worker_delay_max = _env_float("WORKER_DELAY_MAX", 1.5)
        s.album_buffer_seconds = _env_float("ALBUM_BUFFER_SECONDS", 2.0)
        s.health_check_interval = _env_int("HEALTH_CHECK_INTERVAL", 300)
        s.max_retries = _env_int("MAX_RETRIES", 3)
        s.log_level = os.getenv("LOG_LEVEL", "INFO").upper()

        s.listener_silence_timeout = _env_int("LISTENER_SILENCE_TIMEOUT", 900)

        s.daily_report_enabled = _env_bool("DAILY_REPORT_ENABLED", True)
        s.daily_report_hour = _env_int("DAILY_REPORT_HOUR", 8)
        s.daily_report_timezone = os.getenv("DAILY_REPORT_TIMEZONE", "Etc/GMT-3")
        s.daily_report_run_on_start = _env_bool("DAILY_REPORT_RUN_ON_START", False)

        if not s.api_id:
            raise EnvironmentError("Missing required environment variable: API_ID")
        if not s.alert_chat_id:
            raise EnvironmentError("Missing required environment variable: ALERT_CHAT_ID")

    def _load_channels(self):
        """Load channel routing from channels.yml."""
        path = self.CHANNELS_PATH
        if not os.path.exists(path):
            logger.warning(f"Channels config not found at {path} — using empty config")
            return

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

            # Source channel
            source = data.get("source", {})
            if source.get("chat_id") and not self.settings.source_chat_id:
                self.settings.source_chat_id = int(source["chat_id"])

            # Destinations
            self.settings.destinations = []
            for dest in data.get("destinations", []):
                self.settings.destinations.append(
                    Destination(
                        chat_id=int(dest["chat_id"]),
                        name=dest.get("name", str(dest["chat_id"])),
                        enabled=dest.get("enabled", True),
                    )
                )

            # Forward attribution (preserve "Forwarded from" tag)
            fa_data = data.get("forward_attribution") or {}
            allowed: List[OriginChannel] = []
            for origin in fa_data.get("allowed_origin_channels") or []:
                allowed.append(
                    OriginChannel(
                        chat_id=int(origin["chat_id"]),
                        name=origin.get("name", str(origin["chat_id"])),
                    )
                )
            self.settings.forward_attribution = ForwardAttributionSettings(
                enabled=fa_data.get("enabled", True),
                allowed_origin_channels=allowed,
            )

            self._channels_mtime = os.path.getmtime(path)
            logger.info(
                f"Loaded channels.yml: source={self.settings.source_chat_id}, "
                f"destinations={len(self.settings.destinations)}, "
                f"forward_attribution.enabled={self.settings.forward_attribution.enabled}, "
                f"allowed_origins={len(allowed)}"
            )

        except Exception as e:
            logger.error(f"Failed to load channels.yml: {e}")

    def _load_replacements(self):
        """Load word replacement rules from replacements.yml."""
        path = self.REPLACEMENTS_PATH
        if not os.path.exists(path):
            logger.warning(f"Replacements config not found at {path} — no rules loaded")
            return

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

            self.settings.replacement_rules = []
            for rule in data.get("rules", []):
                self.settings.replacement_rules.append(
                    ReplacementRule(
                        pattern=rule["pattern"],
                        replacement=rule.get("replacement", ""),
                        is_regex=rule.get("is_regex", False),
                    )
                )

            self._replacements_mtime = os.path.getmtime(path)
            logger.info(f"Loaded replacements.yml: {len(self.settings.replacement_rules)} rules")

        except Exception as e:
            logger.error(f"Failed to load replacements.yml: {e}")

    def check_and_reload(self):
        """
        Check if YAML config files have changed and reload if needed.
        Called by health.py every health check cycle (5 min).
        Returns True if any config was reloaded.
        """
        reloaded = False

        try:
            if os.path.exists(self.CHANNELS_PATH):
                mtime = os.path.getmtime(self.CHANNELS_PATH)
                if mtime != self._channels_mtime:
                    self._load_channels()
                    reloaded = True
                    logger.info("Hot-reloaded channels.yml")
        except Exception as e:
            logger.error(f"Error checking channels.yml for reload: {e}")

        try:
            if os.path.exists(self.REPLACEMENTS_PATH):
                mtime = os.path.getmtime(self.REPLACEMENTS_PATH)
                if mtime != self._replacements_mtime:
                    self._load_replacements()
                    reloaded = True
                    logger.info("Hot-reloaded replacements.yml")
        except Exception as e:
            logger.error(f"Error checking replacements.yml for reload: {e}")

        return reloaded

    def get_active_destinations(self) -> List[Destination]:
        """Return only enabled destinations."""
        return [d for d in self.settings.destinations if d.enabled]

    def validate(self) -> List[str]:
        """
        Validate configuration completeness. Returns list of error messages.
        Used by startup self-test.
        """
        errors = []
        s = self.settings

        if not s.api_id:
            errors.append("API_ID is not set")
        if not s.api_hash:
            errors.append("API_HASH is not set")
        if not s.source_chat_id:
            errors.append("Source chat ID not configured (set SOURCE_CHAT_ID or channels.yml)")
        if not s.destinations:
            errors.append("No destinations configured in channels.yml")
        if not s.webhook_secret:
            errors.append("WEBHOOK_SECRET is not set")
        if not s.bot_token:
            errors.append("BOT_TOKEN is not set")
        if not s.alert_bot_token:
            errors.append("ALERT_BOT_TOKEN is not set")
        if not s.alert_chat_id:
            errors.append("ALERT_CHAT_ID is not set")

        return errors


# Singleton instance
config = Config()
