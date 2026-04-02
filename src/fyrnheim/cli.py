"""fyrnheim CLI -- the fyr command."""

from __future__ import annotations

import importlib.resources
import logging
import shutil
import sys
from pathlib import Path

import click

from fyrnheim import __version__

_SCAFFOLD_PKG = "fyrnheim._scaffold"


# ---------------------------------------------------------------------------
# Scaffold helpers
# ---------------------------------------------------------------------------


def _scaffold_project(target: Path, *, named: bool) -> None:
    """Create project structure with scaffold files in target directory."""
    if named:
        target.mkdir(parents=True, exist_ok=True)
        click.echo(f"Created {target.name}/")
    else:
        click.echo("Initializing in current directory...")

    for subdir in ("entities", "data", "generated", "tests"):
        d = target / subdir
        if not d.exists():
            d.mkdir(parents=True)
            click.echo(f"  created  {subdir}/")

    scaffold = importlib.resources.files(_SCAFFOLD_PKG)
    _copy_scaffold(scaffold, "fyrnheim.yaml", target / "fyrnheim.yaml")
    _copy_scaffold(scaffold, "customers_entity.py", target / "entities" / "customers.py")
    _copy_scaffold(scaffold, "customers.parquet", target / "data" / "customers.parquet")
    _copy_scaffold(scaffold, "test_customers.py", target / "tests" / "test_customers.py")

    click.echo("")
    click.echo("Next steps:")
    if named:
        click.echo(f"  cd {target.name}")
    click.echo("  fyr generate")
    click.echo("  fyr run")


def _copy_scaffold(scaffold: importlib.resources.abc.Traversable, src: str, dest: Path) -> None:
    """Copy a scaffold file to dest, skipping if it already exists."""
    rel = dest.relative_to(dest.parent.parent) if dest.parent.name else dest.name
    if dest.exists():
        click.echo(f"  skipped  {rel} (already exists)")
        return
    source = scaffold.joinpath(src)
    with importlib.resources.as_file(source) as src_path:
        shutil.copy2(src_path, dest)
    click.echo(f"  created  {rel}")


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group()
@click.version_option(version=__version__, prog_name="fyr")
@click.option("-v", "--verbose", is_flag=True, help="Show full tracebacks and debug logging.")
@click.pass_context
def main(ctx: click.Context, verbose: bool) -> None:
    """Define typed entities, generate transformations, run anywhere."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose

    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(name)s: %(message)s" if verbose else "%(message)s",
        force=True,
    )


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@main.command()
@click.argument("project_name", required=False, default=None)
def init(project_name: str | None) -> None:
    """Create a new fyrnheim project with sample entity and data."""
    target = Path(project_name) if project_name else Path.cwd()
    _scaffold_project(target, named=project_name is not None)


@main.command()
def generate() -> None:
    """Generate transformation code from entity definitions."""
    click.echo("Error: generate command is not available (old pipeline removed).", err=True)
    sys.exit(1)


@main.command()
def run() -> None:
    """Execute the pipeline."""
    click.echo("Error: run command is not available (old pipeline removed).", err=True)
    sys.exit(1)
