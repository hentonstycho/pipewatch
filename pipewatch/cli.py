"""CLI entry point for pipewatch.

Provides the main `pipewatch` command with subcommands for checking
pipeline health and sending test notifications.
"""

import sys
import click
from pathlib import Path
from typing import Optional

from pipewatch.config import load_config
from pipewatch.checker import check_all_pipelines
from pipewatch.notifier import dispatch_notifications


DEFAULT_CONFIG_PATH = "pipewatch.yaml"


@click.group()
@click.version_option(version="0.1.0", prog_name="pipewatch")
def cli():
    """pipewatch — Monitor and alert on ETL pipeline health."""
    pass


@cli.command("check")
@click.option(
    "--config",
    "-c",
    default=DEFAULT_CONFIG_PATH,
    show_default=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the pipewatch YAML config file.",
)
@click.option(
    "--pipeline",
    "-p",
    default=None,
    help="Run checks for a single named pipeline only.",
)
@click.option(
    "--notify/--no-notify",
    default=True,
    show_default=True,
    help="Send notifications for failing pipelines.",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    default=False,
    help="Suppress output; exit code signals overall health.",
)
def check(config: str, pipeline: Optional[str], notify: bool, quiet: bool):
    """Check pipeline health against configured thresholds.

    Exits with code 0 if all pipelines are healthy, 1 if any fail.
    """
    cfg = load_config(Path(config))

    pipelines_to_check = cfg.pipelines
    if pipeline:
        pipelines_to_check = [
            p for p in cfg.pipelines if p.name == pipeline
        ]
        if not pipelines_to_check:
            click.echo(
                f"Error: no pipeline named '{pipeline}' found in config.",
                err=True,
            )
            sys.exit(2)

    results = check_all_pipelines(pipelines_to_check)

    any_failing = False
    for result in results:
        status_icon = "✅" if result.healthy else "❌"
        if not quiet:
            click.echo(f"{status_icon}  {result.pipeline_name}: {result.message}")
        if not result.healthy:
            any_failing = True

    if notify and any_failing:
        failing = [r for r in results if not r.healthy]
        dispatch_notifications(failing, cfg.notifications)
        if not quiet:
            click.echo(f"\nNotifications dispatched for {len(failing)} failing pipeline(s).")

    sys.exit(1 if any_failing else 0)


@cli.command("validate")
@click.option(
    "--config",
    "-c",
    default=DEFAULT_CONFIG_PATH,
    show_default=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the pipewatch YAML config file.",
)
def validate(config: str):
    """Validate the pipewatch config file without running checks."""
    try:
        cfg = load_config(Path(config))
        click.echo(
            f"✅  Config valid — {len(cfg.pipelines)} pipeline(s) configured."
        )
    except Exception as exc:  # noqa: BLE001
        click.echo(f"❌  Config invalid: {exc}", err=True)
        sys.exit(1)


def main():
    """Package entry point."""
    cli()


if __name__ == "__main__":
    main()
