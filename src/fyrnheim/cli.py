"""fyrnheim CLI -- the fyr command."""

from __future__ import annotations

import importlib.resources
import logging
import shutil
import sys
import time
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

    for subdir in ("entities", "data", "tests"):
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
    click.echo("  Edit entities/customers.py to define your pipeline")
    click.echo("  Run tests with: pytest tests/")


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


def _discover_assets(entities_dir: Path) -> dict[str, list]:
    """Discover pipeline assets by importing Python files from entities_dir.

    Scans for module-level variables of known asset types.
    """
    import importlib.util
    import sys

    from fyrnheim.core.activity import ActivityDefinition
    from fyrnheim.core.analytics_entity import AnalyticsEntity
    from fyrnheim.core.identity import IdentityGraph
    from fyrnheim.core.metrics_model import MetricsModel
    from fyrnheim.core.source import EventSource, StateSource

    assets: dict[str, list] = {
        "sources": [],
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": [],
        "metrics_models": [],
    }

    if not entities_dir.is_dir():
        return assets

    type_map: dict[type, tuple[str, bool]] = {
        StateSource: ("sources", False),
        EventSource: ("sources", False),
        ActivityDefinition: ("activities", False),
        IdentityGraph: ("identity_graphs", False),
        AnalyticsEntity: ("analytics_entities", False),
        MetricsModel: ("metrics_models", False),
    }

    for py_file in sorted(entities_dir.glob("*.py")):
        module_name = f"_fyrnheim_entity_{py_file.stem}"
        spec = importlib.util.spec_from_file_location(module_name, py_file)
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)  # type: ignore[union-attr]
        except Exception as exc:
            logging.getLogger("fyrnheim").warning(
                "Failed to import %s: %s", py_file.name, exc
            )
            continue

        for attr_name in dir(module):
            if attr_name.startswith("_"):
                continue
            val = getattr(module, attr_name)

            # Check single instances
            for typ, (key, _) in type_map.items():
                if isinstance(val, typ):
                    assets[key].append(val)
                    break

            # Check lists
            if isinstance(val, list) and val:
                for typ, (key, _) in type_map.items():
                    if isinstance(val[0], typ):
                        assets[key].extend(val)
                        break

    # Deduplicate by name — same source can be defined in multiple files
    for key in assets:
        seen_names: set[str] = set()
        deduped: list = []
        for item in assets[key]:
            item_name = getattr(item, "name", None) or str(id(item))
            if item_name not in seen_names:
                seen_names.add(item_name)
                deduped.append(item)
        assets[key] = deduped

    return assets


@main.command()
@click.option(
    "--entities-dir", default="entities", help="Directory with entity definitions"
)
@click.option("--output", "-o", default=None, help="Output file (default: open in browser)")
@click.pass_context
def dag(ctx: click.Context, entities_dir: str, output: str | None) -> None:
    """Generate and display the pipeline DAG."""
    import tempfile
    import webbrowser

    from fyrnheim.visualization import generate_dag_html

    edir = Path(entities_dir)
    assets = _discover_assets(edir)

    html_content = generate_dag_html(
        sources=assets["sources"],
        activities=assets["activities"],
        identity_graphs=assets["identity_graphs"],
        analytics_entities=assets["analytics_entities"],
        metrics_models=assets["metrics_models"],
    )

    if output:
        out_path = Path(output)
        out_path.write_text(html_content, encoding="utf-8")
        click.echo(f"DAG written to {out_path}")
    else:
        with tempfile.NamedTemporaryFile(
            suffix=".html", delete=False, mode="w", encoding="utf-8"
        ) as f:
            f.write(html_content)
            tmp_path = f.name
        click.echo(f"Opening DAG in browser: {tmp_path}")
        webbrowser.open(f"file://{tmp_path}")


@main.command()
@click.option(
    "--entities-dir", default=None, help="Directory with entity definitions"
)
@click.option(
    "--data-dir", default=None, help="Directory with source data files"
)
@click.option(
    "--output-dir", default=None, help="Directory for pipeline output"
)
@click.option(
    "--backend", default=None, help="Execution backend (duckdb, clickhouse)"
)
@click.pass_context
def run(
    ctx: click.Context,
    entities_dir: str | None,
    data_dir: str | None,
    output_dir: str | None,
    backend: str | None,
) -> None:
    """Run the pipeline: load sources, apply transformations, write output."""
    from fyrnheim.config import resolve_config
    from fyrnheim.engine.executor import IbisExecutor
    from fyrnheim.engine.pipeline import run_pipeline

    verbose = ctx.obj.get("verbose", False)

    try:
        config = resolve_config(
            entities_dir=entities_dir,
            data_dir=data_dir,
            output_dir=output_dir,
            backend=backend,
        )
    except Exception as exc:
        if verbose:
            raise
        click.echo(f"Configuration error: {exc}", err=True)
        sys.exit(1)

    # Discover assets
    assets = _discover_assets(config.entities_dir)

    total_assets = sum(len(v) for v in assets.values())
    if total_assets == 0:
        click.echo(f"No assets found in {config.entities_dir}")
        sys.exit(1)

    # Print discovery summary
    click.echo(f"Discovered: {len(assets['sources'])} sources, "
               f"{len(assets['activities'])} activities, "
               f"{len(assets['identity_graphs'])} identity graphs, "
               f"{len(assets['analytics_entities'])} analytics entities, "
               f"{len(assets['metrics_models'])} metrics models")

    # Create executor and run
    executor = IbisExecutor.from_config(
        backend=config.backend,
        backend_config=config.backend_config,
    )
    start = time.monotonic()

    try:
        result = run_pipeline(assets, config, executor)
    except Exception as exc:
        if verbose:
            raise
        click.echo(f"Pipeline failed: {exc}", err=True)
        sys.exit(1)

    elapsed = time.monotonic() - start

    # Print summary
    click.echo("")
    click.echo(f"Sources processed: {result.source_count}")
    click.echo(f"Outputs written:   {result.output_count}")

    if result.outputs:
        click.echo("")
        for name, row_count in result.outputs.items():
            click.echo(f"  {name}: {row_count} rows -> {config.output_dir / name}.parquet")

    click.echo(f"\nCompleted in {elapsed:.2f}s")

    if result.errors:
        click.echo("")
        click.echo(f"Errors ({len(result.errors)}):")
        for err in result.errors:
            if verbose:
                click.echo(f"  - {err}", err=True)
            else:
                # Show a shorter version without the full traceback
                click.echo(f"  - {err}", err=True)
        sys.exit(1)


