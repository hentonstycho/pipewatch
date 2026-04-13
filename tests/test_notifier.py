"""Tests for pipewatch.notifier."""

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.checker import CheckResult
from pipewatch.config import NotificationConfig
from pipewatch.notifier import (
    _format_message,
    dispatch_notifications,
    send_email,
    send_slack,
)


@pytest.fixture()
def failing_result():
    return CheckResult(
        pipeline_name="orders",
        healthy=False,
        violations=["row_count 50 below min_rows 100"],
    )


@pytest.fixture()
def ok_result():
    return CheckResult(pipeline_name="users", healthy=True, violations=[])


def test_format_message_includes_pipeline_name(failing_result):
    msg = _format_message([failing_result])
    assert "orders" in msg
    assert "row_count 50 below min_rows 100" in msg


def test_format_message_marks_ok(ok_result):
    msg = _format_message([ok_result])
    assert "[OK]" in msg


def test_format_message_marks_fail(failing_result):
    msg = _format_message([failing_result])
    assert "[FAIL]" in msg


@patch("pipewatch.notifier.urllib.request.urlopen")
def test_send_slack_success(mock_urlopen, failing_result):
    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    mock_urlopen.return_value = mock_resp

    result = send_slack("https://hooks.slack.com/test", [failing_result])
    assert result is True
    mock_urlopen.assert_called_once()


@patch("pipewatch.notifier.urllib.request.urlopen", side_effect=OSError("timeout"))
def test_send_slack_failure(mock_urlopen, failing_result):
    result = send_slack("https://hooks.slack.com/test", [failing_result])
    assert result is False


@patch("pipewatch.notifier.smtplib.SMTP")
def test_send_email_success(mock_smtp_cls, failing_result):
    cfg = NotificationConfig(
        slack_webhook=None,
        email_recipients=["ops@example.com"],
        email_from="pipewatch@example.com",
        smtp_host="smtp.example.com",
        smtp_port=587,
    )
    mock_server = MagicMock()
    mock_smtp_cls.return_value.__enter__ = lambda s: mock_server
    mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

    result = send_email(cfg, [failing_result])
    assert result is True


def test_send_email_no_recipients(failing_result):
    cfg = NotificationConfig(
        slack_webhook=None,
        email_recipients=[],
        email_from=None,
        smtp_host=None,
        smtp_port=None,
    )
    result = send_email(cfg, [failing_result])
    assert result is False


@patch("pipewatch.notifier.send_slack")
@patch("pipewatch.notifier.send_email")
def test_dispatch_skips_when_all_healthy(mock_email, mock_slack, ok_result):
    cfg = NotificationConfig(
        slack_webhook="https://hooks.slack.com/x",
        email_recipients=["a@b.com"],
        email_from=None,
        smtp_host=None,
        smtp_port=None,
    )
    dispatch_notifications(cfg, [ok_result])
    mock_slack.assert_not_called()
    mock_email.assert_not_called()


@patch("pipewatch.notifier.send_slack")
@patch("pipewatch.notifier.send_email")
def test_dispatch_calls_both_when_configured(mock_email, mock_slack, failing_result):
    cfg = NotificationConfig(
        slack_webhook="https://hooks.slack.com/x",
        email_recipients=["a@b.com"],
        email_from=None,
        smtp_host=None,
        smtp_port=None,
    )
    dispatch_notifications(cfg, [failing_result])
    mock_slack.assert_called_once()
    mock_email.assert_called_once()
