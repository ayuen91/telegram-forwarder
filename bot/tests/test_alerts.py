"""Tests for Alert Bot inline keyboard buttons and command listener."""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from alerts import (
    AlertBotCommandListener,
    get_admin_inline_keyboard,
    send_alert,
)


class TestAlertInlineKeyboard:
    def test_get_admin_inline_keyboard(self):
        markup = get_admin_inline_keyboard()
        assert "inline_keyboard" in markup
        rows = markup["inline_keyboard"]
        assert len(rows) >= 4
        # Verify button callback_data targets
        callbacks = [btn["callback_data"] for row in rows for btn in row]
        assert "cmd_status" in callbacks
        assert "cmd_stats" in callbacks
        assert "cmd_reload" in callbacks
        assert "cmd_retry" in callbacks
        assert "cmd_deadletter" in callbacks
        assert "cmd_clear_deadletter" in callbacks
        assert "cmd_clear_queues" in callbacks
        assert "cmd_help" in callbacks

    @pytest.mark.asyncio
    async def test_send_alert_includes_inline_keyboard_by_default(self):
        with patch("alerts.aiohttp.ClientSession") as mock_session_cls:
            mock_session = MagicMock()
            mock_resp = AsyncMock()
            mock_resp.status = 200
            mock_post_cm = MagicMock()
            mock_post_cm.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_post_cm.__aexit__ = AsyncMock(return_value=None)
            mock_session.post.return_value = mock_post_cm
            mock_session_cls.return_value.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session_cls.return_value.__aexit__ = AsyncMock(return_value=None)

            await send_alert(
                token="test_token",
                chat_id=12345,
                message="Test alert",
                alert_key="test_key_unique_1",
            )

            mock_session.post.assert_called_once()
            call_kwargs = mock_session.post.call_args.kwargs
            payload = call_kwargs["json"]
            assert payload["chat_id"] == 12345
            assert "reply_markup" in payload
            assert payload["reply_markup"] == get_admin_inline_keyboard()

    @pytest.mark.asyncio
    async def test_listener_callback_query_dispatch(self):
        listener = AlertBotCommandListener(
            token="test_token",
            chat_id=12345,
            health_monitor=None,
            config=None,
            queue_mgr=None,
            redis_client=None,
            shutdown_event=None,
        )

        listener._answer_callback_query = AsyncMock()
        listener._cmd_status = AsyncMock()

        session = AsyncMock()
        await listener._handle_callback("cmd_status", session)
        listener._cmd_status.assert_awaited_once_with(session)

    @pytest.mark.asyncio
    async def test_setup_bot_commands(self):
        listener = AlertBotCommandListener(
            token="test_token",
            chat_id=12345,
            health_monitor=None,
            config=None,
            queue_mgr=None,
            redis_client=None,
            shutdown_event=None,
        )

        session = MagicMock()
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_post_cm = MagicMock()
        mock_post_cm.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_post_cm.__aexit__ = AsyncMock(return_value=None)
        session.post.return_value = mock_post_cm

        await listener._setup_bot_commands(session)

        session.post.assert_called_once()
        url_called = session.post.call_args.args[0]
        assert "setMyCommands" in url_called
        payload = session.post.call_args.kwargs["json"]
        assert "commands" in payload
        cmds = [c["command"] for c in payload["commands"]]
        assert "status" in cmds
        assert "stats" in cmds
        assert "reload" in cmds
        assert "help" in cmds
