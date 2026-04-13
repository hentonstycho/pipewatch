"""Tests for pipewatch configuration loading."""

import os
import textwrap
import pytest

from pipewatch.config import load_config, PipewatchConfig, ThresholdConfig, NotificationConfig


SAMPLE_YAML = textwrap.dedent("""\
    pipelines:
      - name: daily_sales
        thresholds:
          max_duration_seconds: 300
          min_rows_processed: 1000
          max_error_rate: 0.05
      - name: user_sync
        thresholds:
          max_duration_seconds: 60
    notifications:
      slack_webhook_url: https://hooks.slack.com/services/TEST
      email_recipients:
        - ops@example.com
      smtp_host: smtp.example.com
      smtp_port: 465
""")


@pytest.fixture
def config_file(tmp_path):
    cfg = tmp_path / "pipewatch.yaml"
    cfg.write_text(SAMPLE_YAML)
    return str(cfg)


def test_load_config_returns_pipewatch_config(config_file):
    cfg = load_config(config_file)
    assert isinstance(cfg, PipewatchConfig)


def test_pipelines_parsed_correctly(config_file):
    cfg = load_config(config_file)
    assert len(cfg.pipelines) == 2
    assert cfg.pipelines[0].name == "daily_sales"
    assert cfg.pipelines[1].name == "user_sync"


def test_thresholds_parsed(config_file):
    cfg = load_config(config_file)
    t = cfg.pipelines[0].thresholds
    assert isinstance(t, ThresholdConfig)
    assert t.max_duration_seconds == 300
    assert t.min_rows_processed == 1000
    assert t.max_error_rate == 0.05


def test_missing_threshold_is_none(config_file):
    cfg = load_config(config_file)
    t = cfg.pipelines[1].thresholds
    assert t.min_rows_processed is None
    assert t.max_error_rate is None


def test_notifications_parsed(config_file):
    cfg = load_config(config_file)
    n = cfg.notifications
    assert isinstance(n, NotificationConfig)
    assert n.slack_webhook_url == "https://hooks.slack.com/services/TEST"
    assert n.email_recipients == ["ops@example.com"]
    assert n.smtp_host == "smtp.example.com"
    assert n.smtp_port == 465


def test_missing_config_file_raises():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/pipewatch.yaml")


def test_invalid_yaml_raises(tmp_path):
    bad = tmp_path / "pipewatch.yaml"
    bad.write_text("- just a list\n- not a mapping\n")
    with pytest.raises(ValueError, match="YAML mapping"):
        load_config(str(bad))
