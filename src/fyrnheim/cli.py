"""fyrnheim CLI -- the fyr command."""

from __future__ import annotations

import dataclasses
import importlib.resources
import json
import logging
import shutil
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

import click

from fyrnheim import __version__

if TYPE_CHECKING:
    from fyrnheim.config import ResolvedConfig
    from fyrnheim.engine.pipeline import PipelineTimings

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
    from fyrnheim.core.staging_view import StagingView

    assets: dict[str, list] = {
        "sources": [],
        "activities": [],
        "identity_graphs": [],
        "analytics_entities": [],
        "metrics_models": [],
        "staging_views": [],
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
        StagingView: ("staging_views", False),
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
@click.option(
    "--no-state",
    is_flag=True,
    default=False,
    help="Bypass fyrnheim_state table: always re-materialize staging views and skip state writes.",
)
@click.option(
    "--max-parallel-io",
    type=click.IntRange(min=1),
    default=None,
    help=(
        "Max concurrent I/O tasks for source loads and entity/metrics "
        "writes (default 4). Set to 1 for strictly serial execution."
    ),
)
@click.pass_context
def run(
    ctx: click.Context,
    entities_dir: str | None,
    data_dir: str | None,
    output_dir: str | None,
    backend: str | None,
    no_state: bool,
    max_parallel_io: int | None,
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
            max_parallel_io=max_parallel_io,
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
        result = run_pipeline(assets, config, executor, no_state=no_state)
    except Exception as exc:
        if verbose:
            raise
        click.echo(f"Pipeline failed: {exc}", err=True)
        sys.exit(1)

    elapsed = time.monotonic() - start

    # Print summary
    click.echo("")
    click.echo(f"Sources processed: {result.source_count}")
    if len(result.staging_materialized) + len(result.staging_skipped) > 0:
        click.echo(
            f"Staging views: {len(result.staging_materialized)} materialized, "
            f"{len(result.staging_skipped)} skipped"
        )
    click.echo(f"Outputs written:   {result.output_count}")

    if result.outputs:
        click.echo("")
        for name, row_count in result.outputs.items():
            destination = result.output_destinations.get(name)
            if destination is None:
                destination = f"parquet:{config.output_dir / name}.parquet"
            click.echo(f"  {name}: {row_count} rows -> {destination}")

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


def _resolve_and_discover(
    ctx: click.Context,
    entities_dir: str | None,
    data_dir: str | None = None,
    output_dir: str | None = None,
    backend: str | None = None,
    max_parallel_io: int | None = None,
) -> tuple[ResolvedConfig, dict[str, list]]:
    """Shared helper: resolve config + discover assets, or exit(1) on error."""
    from fyrnheim.config import resolve_config

    verbose = ctx.obj.get("verbose", False)
    try:
        config = resolve_config(
            entities_dir=entities_dir,
            data_dir=data_dir,
            output_dir=output_dir,
            backend=backend,
            max_parallel_io=max_parallel_io,
        )
    except Exception as exc:
        if verbose:
            raise
        click.echo(f"Configuration error: {exc}", err=True)
        sys.exit(1)

    assets = _discover_assets(config.entities_dir)
    return config, assets


def _format_bench_report(timings: PipelineTimings, elapsed_seconds: float) -> str:
    """Render a human-readable phase-ordered `fyr bench` report.

    Plain f-strings only — no color, no rich/tables. The format mirrors
    the M058 story's example output.
    """
    lines: list[str] = [f"Pipeline bench — total {elapsed_seconds:.3f}s"]
    lines.append(f"  staging_views: {timings.staging_views_s:.3f}s")

    def _name_width(names: list[str]) -> int:
        return max((len(n) for n in names), default=0)

    lines.append(f"  source_loads ({len(timings.source_loads)}):")
    if timings.source_loads:
        width = _name_width(list(timings.source_loads))
        for name, secs in timings.source_loads.items():
            lines.append(f"    {(name + ':').ljust(width + 1)} {secs:.3f}s")

    lines.append(f"  activities:      {timings.activities_s:.3f}s")

    lines.append(f"  identity_graphs ({len(timings.identity_graphs)}):")
    if timings.identity_graphs:
        width = _name_width(list(timings.identity_graphs))
        for name, secs in timings.identity_graphs.items():
            lines.append(f"    {(name + ':').ljust(width + 1)} {secs:.3f}s")

    lines.append(f"  analytics_entities ({len(timings.analytics_entities)}):")
    if timings.analytics_entities:
        width = _name_width(list(timings.analytics_entities))
        for name, parts in timings.analytics_entities.items():
            lines.append(
                f"    {(name + ':').ljust(width + 1)} "
                f"project={parts.get('project_s', 0.0):.3f}s  "
                f"write={parts.get('write_s', 0.0):.3f}s"
            )

    if timings.metrics_models:
        lines.append(f"  metrics_models ({len(timings.metrics_models)}):")
        width = _name_width(list(timings.metrics_models))
        for name, parts in timings.metrics_models.items():
            lines.append(
                f"    {(name + ':').ljust(width + 1)} "
                f"project={parts.get('project_s', 0.0):.3f}s  "
                f"write={parts.get('write_s', 0.0):.3f}s"
            )
    else:
        lines.append("  metrics_models (0): (none)")

    return "\n".join(lines)


@main.command()
@click.option("--entities-dir", default=None, help="Directory with entity definitions")
@click.option("--data-dir", default=None, help="Directory with source data files")
@click.option("--output-dir", default=None, help="Directory for pipeline output")
@click.option("--backend", default=None, help="Execution backend (duckdb, clickhouse)")
@click.option(
    "--no-state",
    is_flag=True,
    default=False,
    help="Bypass fyrnheim_state table: always re-materialize staging views and skip state writes.",
)
@click.option(
    "--json",
    "as_json",
    is_flag=True,
    default=False,
    help="Emit PipelineTimings as JSON on stdout (nothing else).",
)
@click.option(
    "--max-parallel-io",
    type=click.IntRange(min=1),
    default=None,
    help=(
        "Max concurrent I/O tasks for source loads and entity/metrics "
        "writes (default 4). Set to 1 for strictly serial execution."
    ),
)
@click.pass_context
def bench(
    ctx: click.Context,
    entities_dir: str | None,
    data_dir: str | None,
    output_dir: str | None,
    backend: str | None,
    no_state: bool,
    as_json: bool,
    max_parallel_io: int | None,
) -> None:
    """Run the pipeline and print per-phase timings (bench harness)."""
    from fyrnheim.engine.executor import IbisExecutor
    from fyrnheim.engine.pipeline import run_pipeline

    verbose = ctx.obj.get("verbose", False)

    config, assets = _resolve_and_discover(
        ctx,
        entities_dir,
        data_dir=data_dir,
        output_dir=output_dir,
        backend=backend,
        max_parallel_io=max_parallel_io,
    )

    total_assets = sum(len(v) for v in assets.values())
    if total_assets == 0:
        click.echo(f"No assets found in {config.entities_dir}", err=True)
        sys.exit(1)

    executor = IbisExecutor.from_config(
        backend=config.backend,
        backend_config=config.backend_config,
    )

    try:
        try:
            result = run_pipeline(assets, config, executor, no_state=no_state)
        except Exception as exc:
            if verbose:
                raise
            click.echo(f"Pipeline failed: {exc}", err=True)
            sys.exit(1)

        if as_json:
            click.echo(json.dumps(dataclasses.asdict(result.timings)))
        else:
            click.echo(_format_bench_report(result.timings, result.elapsed_seconds))

        if result.errors:
            for err in result.errors:
                click.echo(f"  - {err}", err=True)
            sys.exit(1)
    finally:
        executor.close()


@main.command()
@click.option("--entities-dir", default=None, help="Directory with entity definitions")
@click.option("--backend", default=None, help="Execution backend (duckdb, bigquery)")
@click.option("--view", default=None, help="Materialize only the named StagingView")
@click.option(
    "--no-state",
    is_flag=True,
    default=False,
    help="Bypass fyrnheim_state: always re-materialize and skip state writes.",
)
@click.pass_context
def materialize(
    ctx: click.Context,
    entities_dir: str | None,
    backend: str | None,
    view: str | None,
    no_state: bool,
) -> None:
    """Run Phase 0 only: materialize StagingView instances."""
    from fyrnheim.engine.executor import IbisExecutor
    from fyrnheim.engine.staging_runner import materialize_staging_views

    verbose = ctx.obj.get("verbose", False)
    config, assets = _resolve_and_discover(ctx, entities_dir, backend=backend)

    staging_views = list(assets.get("staging_views", []))
    if not staging_views:
        click.echo(f"No staging views found in {config.entities_dir}")
        sys.exit(1)

    if view is not None:
        matching = [v for v in staging_views if v.name == view]
        if not matching:
            names = ", ".join(sorted(v.name for v in staging_views))
            click.echo(
                f"Unknown staging view: {view!r}. Available: {names}", err=True
            )
            sys.exit(1)
        staging_views = matching

    executor = IbisExecutor.from_config(
        backend=config.backend,
        backend_config=config.backend_config,
    )

    try:
        summary = materialize_staging_views(
            executor, staging_views, no_state=no_state
        )
    except Exception as exc:
        if verbose:
            raise
        click.echo(f"Materialization failed: {exc}", err=True)
        sys.exit(1)

    click.echo(
        f"Staging views: {len(summary.materialized)} materialized, "
        f"{len(summary.skipped)} skipped"
    )
    for name in summary.materialized:
        click.echo(f"  materialized  {name}")
    for name in summary.skipped:
        click.echo(f"  skipped       {name}")


@main.command()
@click.option("--entities-dir", default=None, help="Directory with entity definitions")
@click.option("--backend", default=None, help="Execution backend (duckdb, bigquery)")
@click.option("--view", "view", required=True, help="Name of the StagingView to drop")
@click.pass_context
def drop(
    ctx: click.Context,
    entities_dir: str | None,
    backend: str | None,
    view: str,
) -> None:
    """Drop a StagingView and remove its row from fyrnheim_state."""
    from fyrnheim.engine.executor import IbisExecutor
    from fyrnheim.engine.staging_runner import (
        STATE_TABLE_NAME,
        _qualify_state_table,
    )

    verbose = ctx.obj.get("verbose", False)
    config, assets = _resolve_and_discover(ctx, entities_dir, backend=backend)

    staging_views = list(assets.get("staging_views", []))
    matching = [v for v in staging_views if v.name == view]
    if not matching:
        names = ", ".join(sorted(v.name for v in staging_views)) or "(none)"
        click.echo(
            f"Unknown staging view: {view!r}. Available: {names}", err=True
        )
        sys.exit(1)
    target = matching[0]

    executor = IbisExecutor.from_config(
        backend=config.backend,
        backend_config=config.backend_config,
    )

    try:
        executor.drop_view(target.project, target.dataset, target.name)
        # Best-effort: remove state row if the state table exists.
        qualified = _qualify_state_table(executor, target.project, target.dataset)
        try:
            executor.execute_parameterized(
                f"DELETE FROM {qualified} WHERE name = @name",
                {"name": target.name},
            )
        except Exception as state_exc:
            log = logging.getLogger("fyrnheim")
            log.debug(
                "State row delete skipped for %s (table missing?): %s",
                target.name,
                state_exc,
            )
    except Exception as exc:
        if verbose:
            raise
        click.echo(f"Drop failed: {exc}", err=True)
        sys.exit(1)

    click.echo(
        f"Dropped view {target.name} ({target.project}.{target.dataset}); "
        f"removed state row from {STATE_TABLE_NAME}"
    )


@main.command("list-staging")
@click.option("--entities-dir", default=None, help="Directory with entity definitions")
@click.option("--backend", default=None, help="Execution backend (duckdb, bigquery)")
@click.pass_context
def list_staging(
    ctx: click.Context,
    entities_dir: str | None,
    backend: str | None,
) -> None:
    """List discovered StagingView instances with freshness."""
    from fyrnheim.engine.executor import IbisExecutor
    from fyrnheim.engine.staging_runner import _load_state

    config, assets = _resolve_and_discover(ctx, entities_dir, backend=backend)
    staging_views = list(assets.get("staging_views", []))

    if not staging_views:
        click.echo(f"No staging views found in {config.entities_dir}")
        return

    # Try to read state per (project, dataset). Treat errors as "no state".
    executor = IbisExecutor.from_config(
        backend=config.backend,
        backend_config=config.backend_config,
    )
    state_by_key: dict[tuple[str, str], dict[str, str]] = {}
    for v in staging_views:
        key = (v.project, v.dataset)
        if key in state_by_key:
            continue
        try:
            state_by_key[key] = _load_state(executor, v.project, v.dataset)
        except Exception:
            state_by_key[key] = {}

    rows = []
    for v in staging_views:
        stored = state_by_key.get((v.project, v.dataset), {}).get(v.name)
        if stored is None:
            freshness = "unmaterialized"
        elif stored == v.content_hash():
            freshness = "fresh"
        else:
            freshness = "stale"
        rows.append((v.name, v.dataset, freshness))

    name_w = max(len("NAME"), max(len(r[0]) for r in rows))
    ds_w = max(len("DATASET"), max(len(r[1]) for r in rows))
    fr_w = max(len("FRESHNESS"), max(len(r[2]) for r in rows))

    click.echo(f"{'NAME':<{name_w}}  {'DATASET':<{ds_w}}  {'FRESHNESS':<{fr_w}}")
    for name, ds, fr in rows:
        click.echo(f"{name:<{name_w}}  {ds:<{ds_w}}  {fr:<{fr_w}}")


