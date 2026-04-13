"""Configuration loading and validation for pipewatch."""

import os
from dataclasses import dataclass, field
from typing import Optional

import yaml


@dataclass
class ThresholdConfig:
    max_duration_seconds: Optional[float] = None
    min_rows_processed: Optional[int] = None
    max_error_rate: Optional[float] = None


@dataclass
class NotificationConfig:
    slack_webhook_url: Optional[str] = None
    email_recipients: list = field(default_factory=list)
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None


@dataclass
class PipelineConfig:
    name: str
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)


@dataclass
class PipewatchConfig:
    pipelines: list = field(default_factory=list)
    notifications: NotificationConfig = field(default_factory=NotificationConfig)


def load_config(path: str = "pipewatch.yaml") -> PipewatchConfig:
    """Load and parse pipewatch configuration from a YAML file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError("Config file must be a YAML mapping.")

    notifications_raw = raw.get("notifications", {})
    notifications = NotificationConfig(
        slack_webhook_url=notifications_raw.get("slack_webhook_url"),
        email_recipients=notifications_raw.get("email_recipients", []),
        smtp_host=notifications_raw.get("smtp_host"),
        smtp_port=notifications_raw.get("smtp_port", 587),
        smtp_user=notifications_raw.get("smtp_user"),
        smtp_password=notifications_raw.get("smtp_password"),
    )

    pipelines = []
    for p in raw.get("pipelines", []):
        thresholds_raw = p.get("thresholds", {})
        thresholds = ThresholdConfig(
            max_duration_seconds=thresholds_raw.get("max_duration_seconds"),
            min_rows_processed=thresholds_raw.get("min_rows_processed"),
            max_error_rate=thresholds_raw.get("max_error_rate"),
        )
        pipelines.append(PipelineConfig(name=p["name"], thresholds=thresholds))

    return PipewatchConfig(pipelines=pipelines, notifications=notifications)
