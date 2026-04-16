"""CLI commands for anomaly detection."""
from __future__ import annotations

import sys
import click

from pipewatch.config import load_config
from pipewatch.anomaly import detect_all_anomalies, detect_anomaly


@click.group("anomaly")
def anomaly_cmd():
    """Detect failure-rate anomalies in pipelines."""


@anomaly_cmd.command("check")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--pipeline", "pipeline_name", default=None, help="Check a single pipeline.")
@click.option("--baseline-window", default=20, show_default=True)
@click.option("--recent-window", default=5, show_default=True)
@click.option("--spike-threshold", default=2.0, show_default=True)
@click.option("--history-dir", default=".pipewatch/history", show_default=True)
@click.option("--fail-on-anomaly", is_flag=True, default=False)
def check_cmd(
    config_path, pipeline_name, baseline_window, recent_window,
    spike_threshold, history_dir, fail_on_anomaly
):
    """Print anomaly detection results for all (or one) pipeline(s)."""
    cfg = load_config(config_path)
    known = {p.name for p in cfg.pipelines}

    if pipeline_name:
        if pipeline_name not in known:
            click.echo(f"Unknown pipeline: {pipeline_name}", err=True)
            sys.exit(2)
        results = [
            detect_anomaly(
                pipeline_name,
                history_dir=history_dir,
                baseline_window=baseline_window,
                recent_window=recent_window,
                spike_threshold=spike_threshold,
            )
        ]
    else:
        results = detect_all_anomalies(
            cfg,
            history_dir=history_dir,
            baseline_window=baseline_window,
            recent_window=recent_window,
            spike_threshold=spike_threshold,
        )

    any_anomaly = False
    for r in results:
        click.echo(str(r))
        if r.is_anomaly:
            any_anomaly = True

    if fail_on_anomaly and any_anomaly:
        sys.exit(1)
