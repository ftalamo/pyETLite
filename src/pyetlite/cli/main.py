"""PyETLite CLI — pyetlite run / validate / list / schedule."""
import importlib.util
import inspect
import logging
import sys
from pathlib import Path
from typing import Optional

import typer

from pyetlite.core.pipeline import Pipeline

app = typer.Typer(
    name="pyetlite",
    help="Declarative ETL pipelines powered by Polars.",
    add_completion=False,
)


# ── Helpers ───────────────────────────────────────────────────────────

def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )


def _load_pipelines_from_file(path: Path) -> list[Pipeline]:
    """Import a Python file and return all Pipeline objects found in it."""
    if not path.exists():
        typer.echo(f"❌  File not found: {path}", err=True)
        raise typer.Exit(code=1)

    spec = importlib.util.spec_from_file_location("_pyetlite_user_module", path)
    if spec is None or spec.loader is None:
        typer.echo(f"❌  Cannot load file: {path}", err=True)
        raise typer.Exit(code=1)

    module = importlib.util.module_from_spec(spec)
    sys.modules["_pyetlite_user_module"] = module

    try:
        spec.loader.exec_module(module)  # type: ignore[union-attr]
    except Exception as exc:
        typer.echo(f"❌  Error loading {path.name}: {exc}", err=True)
        raise typer.Exit(code=1)

    pipelines = [
        obj for _, obj in inspect.getmembers(module)
        if isinstance(obj, Pipeline)
    ]
    return pipelines


def _echo_header(text: str) -> None:
    typer.echo(f"\n{'─' * 50}")
    typer.echo(f"  {text}")
    typer.echo(f"{'─' * 50}")


# ── Commands ──────────────────────────────────────────────────────────

@app.command()
def run(
    file: Path = typer.Argument(..., help="Python file containing Pipeline objects."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Execute without writing to the sink."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed logs."),
    pipeline_name: Optional[str] = typer.Option(None, "--name", "-n", help="Run only this pipeline by name."),
) -> None:
    """Run all pipelines defined in FILE."""
    _setup_logging(verbose)
    pipelines = _load_pipelines_from_file(file)

    if not pipelines:
        typer.echo(f"⚠️   No Pipeline objects found in {file.name}.")
        raise typer.Exit(code=1)

    if pipeline_name:
        pipelines = [p for p in pipelines if p.name == pipeline_name]
        if not pipelines:
            typer.echo(f"❌  No pipeline named '{pipeline_name}' found in {file.name}.")
            raise typer.Exit(code=1)

    if dry_run:
        for p in pipelines:
            p.dry_run = True

    total = len(pipelines)
    failed = 0

    _echo_header(f"PyETLite — running {total} pipeline(s) from {file.name}")

    for pipeline in pipelines:
        try:
            result = pipeline.run()
            typer.echo(result.summary())
            if not result.success:
                failed += 1
        except Exception as exc:
            typer.echo(f"\n❌  Pipeline '{pipeline.name}' raised: {exc}", err=True)
            failed += 1

    _echo_header(f"Done — {total - failed}/{total} succeeded")

    if failed:
        raise typer.Exit(code=1)


@app.command()
def validate(
    file: Path = typer.Argument(..., help="Python file containing Pipeline objects."),
) -> None:
    """Validate pipeline configuration without executing."""
    pipelines = _load_pipelines_from_file(file)

    if not pipelines:
        typer.echo(f"⚠️   No Pipeline objects found in {file.name}.")
        raise typer.Exit(code=1)

    _echo_header(f"Validating {len(pipelines)} pipeline(s) from {file.name}")

    all_ok = True
    for pipeline in pipelines:
        try:
            pipeline._validate_config()
            typer.echo(f"  ✓  {pipeline.name}")
        except Exception as exc:
            typer.echo(f"  ✗  {pipeline.name} — {exc}")
            all_ok = False

    typer.echo("")
    if all_ok:
        typer.echo("✅  All pipelines are valid.")
    else:
        typer.echo("❌  Some pipelines have configuration errors.")
        raise typer.Exit(code=1)


@app.command(name="list")
def list_pipelines(
    file: Path = typer.Argument(..., help="Python file containing Pipeline objects."),
) -> None:
    """List all pipelines defined in FILE."""
    pipelines = _load_pipelines_from_file(file)

    if not pipelines:
        typer.echo(f"⚠️   No Pipeline objects found in {file.name}.")
        raise typer.Exit(code=1)

    _echo_header(f"Pipelines in {file.name}")
    for p in pipelines:
        dry = " [dry_run]" if p.dry_run else ""
        sched = f" [schedule: {p.schedule}]" if p.schedule else ""
        transforms = len(p._transforms)
        typer.echo(f"  • {p.name:<30} {transforms} transform(s){dry}{sched}")
    typer.echo("")


@app.command()
def schedule(
    file: Path = typer.Argument(..., help="Python file containing Pipeline objects."),
    timezone: str = typer.Option("UTC", "--timezone", "-tz", help="Timezone for cron expressions."),
    db_path: Optional[str] = typer.Option(None, "--db", help="Path to SQLite scheduler DB."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed logs."),
) -> None:
    """Start the scheduler for all pipelines that have a 'schedule' attribute."""
    _setup_logging(verbose)

    from pyetlite.core.scheduler import Scheduler

    pipelines = _load_pipelines_from_file(file)

    if not pipelines:
        typer.echo(f"⚠️   No Pipeline objects found in {file.name}.")
        raise typer.Exit(code=1)

    scheduled = [p for p in pipelines if p.schedule]
    if not scheduled:
        typer.echo("⚠️   No pipelines with a 'schedule' attribute found.")
        typer.echo("     Set schedule='0 3 * * *' on your Pipeline to enable scheduling.")
        raise typer.Exit(code=1)

    db = Path(db_path) if db_path else None
    scheduler = Scheduler(pipelines=scheduled, db_path=db, timezone=timezone)
    scheduler.start()
