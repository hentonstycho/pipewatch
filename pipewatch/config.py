"""Configuration models and loader for pipewatch."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import yaml


@dataclass
class ThresholdConfig:
    min_rows: Optional[int] = None
    max_error_rate: Optional[float] = None
    max_latency_seconds: Optional[int] = None


@dataclass
class NotificationConfig:
    slack_webhook: Optional[str] = None
    email_recipients: List[str] = field(default_factory=list)
    email_from: Optional[str] = None
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None


@dataclass
class PipelineConfig:
    name: str
    source: str
    thresholds: ThresholdConfig = field(default_factory=ThresholdConfig)


@dataclass
class PipewatchConfig:
    pipelines: List[PipelineConfig] = field(default_factory=list)
    notifications: NotificationConfig = field(default_factory=NotificationConfig)


def _parse_thresholds(raw: dict) -> ThresholdConfig:
    return ThresholdConfig(
        min_rows=raw.get("min_rows"),
        max_error_rate=raw.get("max_error_rate"),
        max_latency_seconds=raw.get("max_latency_seconds"),
    )


def _parse_notifications(raw: dict) -> NotificationConfig:
    return NotificationConfig(
        slack_webhook=raw.get("slack_webhook"),
        email_recipients=raw.get("email_recipients") or [],
        email_from=raw.get("email_from"),
        smtp_host=raw.get("smtp_host"),
        smtp_port=raw.get("smtp_port"),
    )


def _parse_pipeline(raw: dict) -> PipelineConfig:
    thresholds_raw = raw.get("thresholds") or {}
    return PipelineConfig(
        name=raw["name"],
        source=raw["source"],
        thresholds=_parse_thresholds(thresholds_raw),
    )


def load_config(path: str | Path = "pipewatch.yaml") -> PipewatchConfig:
    """Load and parse a pipewatch YAML configuration file."""
    with open(path, "r") as fh:
        raw = yaml.safe_load(fh) or {}

    pipelines = [_parse_pipeline(p) for p in raw.get("pipelines") or []]
    notifications = _parse_notifications(raw.get("notifications") or {})
    return PipewatchConfig(pipelines=pipelines, notifications=notifications)
