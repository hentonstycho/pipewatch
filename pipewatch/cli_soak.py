"""cli_soak.py – CLI commands for the soak-window feature."""
from __future__ import annotations

from pathlib import Path

import click

from pipewatch.config import load_config
from pipewatch.soaker import evaluate_all_soaks, evaluate_soak

_SOAK_DIR = Path(".pipewatch/soak")
_HISTORY_DIR = Path(".pipewatch/history")


@click.group(name="soak")
def soak_cmd() -> None:
    """Manage pipeline soak windows."""


@soak_cmd.command(name="status")
@click.option("--config", "config_path", default="pipewatch.yaml", show_default=True)
@click.option("--pipeline", "pipeline_name", default=None, help="Single pipeline name.")
@click.option(
    "--required",
    default=3,
    show_default=True,
    help="Consecutive healthy runs required to leave soak.",
)
@click.option(
    "--fail-soaking",
    is_flag=True,
    default=False,
    help="Exit with code 1 if any pipeline is soaking.",
)
@click.pass_context
def check_cmd(
    ctx: click.Context,
    config_path: str,
    pipeline_name: str | None,
    required: int,
    fail_soaking: bool,
) -> None:
    """Show soak-window status for all (or one) pipeline(s)."""
    cfg = load_config(Path(config_path))

    if pipeline_name:
        names = [p.name for p in cfg.pipelines if p.name == pipeline_name]
        if not names:
            click.echo(f"Unknown pipeline: {pipeline_name}", err=True)
            ctx.exit(2)
            return
    else:
        names = [p.name for p in cfg.pipelines]

    results = evaluate_all_soaks(
        names,
        required=required,
        history_dir=_HISTORY_DIR,
        soak_dir=_SOAK_DIR,
    )

    any_soaking = False
    for r in results:
        icon = "⏳" if r.soaking else "✅"
        click.echo(f"{icon}  {r}")
        if r.soaking:
            any_soaking = True

    if fail_soaking and any_soaking:
        ctx.exit(1)
