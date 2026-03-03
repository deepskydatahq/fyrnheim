"""fyrnheim CLI — the fyr command."""

from __future__ import annotations

import functools
import importlib.resources
import logging
import shutil
import sys
import traceback
from pathlib import Path

import click

from fyrnheim import __version__

_SCAFFOLD_PKG = "fyrnheim._scaffold"

# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def _find_hint(exc: Exception) -> str | None:
    """Return a user-facing hint for a caught exception, or None."""
    msg = str(exc)

    # Engine-specific hints (lazy import to avoid ibis at startup)
    try:
        from fyrnheim.engine.errors import ExecutionError, SourceNotFoundError, TransformModuleError
        from fyrnheim.engine.resolution import CircularDependencyError

        if isinstance(exc, SourceNotFoundError):
            return "Check data_dir in fyrnheim.yaml."
        if isinstance(exc, TransformModuleError):
            return "Run `fyr generate` to regenerate transform code."
        if isinstance(exc, ExecutionError):
            return "Check entity definitions and source data. Use --verbose for details."
        if isinstance(exc, CircularDependencyError):
            return "Check entity source dependencies for cycles."
    except ImportError:
        pass

    if isinstance(exc, FileNotFoundError):
        if "fyrnheim.yaml" in msg:
            return "Run `fyr init` to create a project."
        if "Entities directory" in msg:
            return "Check entities_dir in fyrnheim.yaml."
        return "Check that the path exists. Run `fyr init` to create a project."

    if isinstance(exc, ImportError) and "duckdb" in msg.lower():
        return "Install with: pip install fyrnheim[duckdb]"
    if isinstance(exc, ImportError):
        return "A required dependency may be missing. Try: pip install fyrnheim[duckdb]"

    if isinstance(exc, ValueError) and "Duplicate entity" in msg:
        return None  # message already includes both paths

    if isinstance(exc, ValueError):
        return "Check your fyrnheim.yaml configuration values."

    if isinstance(exc, SyntaxError):
        return None  # message includes file + line

    return "Use --verbose for the full traceback."


def handle_errors(f):
    """Decorator that catches exceptions and prints actionable messages."""

    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except SystemExit:
            raise
        except SyntaxError as exc:
            ctx = click.get_current_context(silent=True)
            verbose = (ctx.obj or {}).get("verbose", False) if ctx else False
            if verbose:
                traceback.print_exc(file=sys.stderr)
            else:
                if exc.filename:
                    click.echo(f"Error loading {exc.filename}: {exc.msg} (line {exc.lineno})", err=True)
                else:
                    click.echo(f"Error: {exc}", err=True)
            raise SystemExit(1) from None
        except Exception as exc:
            ctx = click.get_current_context(silent=True)
            verbose = (ctx.obj or {}).get("verbose", False) if ctx else False

            if verbose:
                traceback.print_exc(file=sys.stderr)
            else:
                click.echo(f"Error: {exc}", err=True)
                hint = _find_hint(exc)
                if hint:
                    click.echo(f"Hint: {hint}", err=True)

            raise SystemExit(1) from None

    return wrapper


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

    for subdir in ("entities", "data", "generated"):
        d = target / subdir
        if not d.exists():
            d.mkdir(parents=True)
            click.echo(f"  created  {subdir}/")

    scaffold = importlib.resources.files(_SCAFFOLD_PKG)
    _copy_scaffold(scaffold, "fyrnheim.yaml", target / "fyrnheim.yaml")
    _copy_scaffold(scaffold, "customers_entity.py", target / "entities" / "customers.py")
    _copy_scaffold(scaffold, "customers.parquet", target / "data" / "customers.parquet")

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
# Run command helpers
# ---------------------------------------------------------------------------

_LAYER_SHORT = {"dimension": "dim"}


def _format_layers(layers: list[str]) -> str:
    return " -> ".join(_LAYER_SHORT.get(layer, layer) for layer in layers)


def _has_quality_failures(entities: list) -> bool:
    for er in entities:
        if er.quality_results:
            for qr in er.quality_results:
                if not qr.passed:
                    return True
    return False


def _print_entity_result(er, registry) -> None:
    info = registry.get(er.entity_name)
    layers = _format_layers(info.layers) if info else ""
    rows = str(er.row_count) if er.row_count is not None else "-"
    status = "ok" if er.status == "success" else er.status.upper()

    click.echo(f"  {er.entity_name:<16s} {layers:<18s} {rows:>6s} rows  {er.duration_seconds:5.1f}s  {status}")

    if er.quality_results:
        passed = sum(1 for qr in er.quality_results if qr.passed)
        failed = sum(1 for qr in er.quality_results if not qr.passed)
        if failed > 0:
            click.echo(f"    checks: {passed} passed, {failed} failed")
            for qr in er.quality_results:
                if not qr.passed:
                    click.echo(f"      {qr.check_name:<42s} FAIL ({qr.failure_count} failures)")


def _print_run_summary(result) -> None:
    parts = [f"{result.success_count} success"]
    if result.error_count > 0:
        parts.append(f"{result.error_count} {'error' if result.error_count == 1 else 'errors'}")
    else:
        parts.append("0 errors")
    if result.skipped_count > 0:
        parts.append(f"{result.skipped_count} skipped")

    click.echo(f"Done: {', '.join(parts)} ({result.total_duration_seconds:.1f}s)")

    if _has_quality_failures(result.entities):
        total_failed = 0
        for er in result.entities:
            if er.quality_results:
                total_failed += sum(1 for qr in er.quality_results if not qr.passed)
        check_word = "check" if total_failed == 1 else "checks"
        click.echo(f"Quality: {total_failed} {check_word} failed")


# ---------------------------------------------------------------------------
# Check command helpers
# ---------------------------------------------------------------------------


def _resolve_target_table(entity_name: str, layers: list[str]) -> str:
    """Determine the DuckDB table name to run checks against."""
    if "snapshot" in layers:
        return f"snapshot_{entity_name}"
    if "dimension" in layers:
        return f"dim_{entity_name}"
    if "prep" in layers:
        return f"prep_{entity_name}"
    return f"dim_{entity_name}"


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
@handle_errors
def init(project_name: str | None) -> None:
    """Create a new fyrnheim project with sample entity and data."""
    target = Path(project_name) if project_name else Path.cwd()
    _scaffold_project(target, named=project_name is not None)


@main.command()
@click.option("--dry-run", is_flag=True, help="Preview generation without writing files.")
@click.option("--entities-dir", type=click.Path(), default=None, help="Entity definitions directory.")
@click.option("--output-dir", type=click.Path(), default=None, help="Output directory for generated modules.")
@handle_errors
def generate(dry_run: bool, entities_dir: str | None, output_dir: str | None) -> None:
    """Generate transformation code from entity definitions."""
    from fyrnheim._generate import generate as _generate_fn
    from fyrnheim.config import resolve_config
    from fyrnheim.engine.registry import EntityRegistry

    cfg = resolve_config(entities_dir=entities_dir, output_dir=output_dir)

    if not cfg.entities_dir.exists():
        click.echo(f"Error: Entities directory not found: {cfg.entities_dir}", err=True)
        raise SystemExit(1)

    registry = EntityRegistry()
    registry.discover(cfg.entities_dir)

    if len(registry) == 0:
        click.echo(f"No entities found in {cfg.entities_dir}")
        return

    click.echo(f"Generating transforms from {cfg.entities_dir}")
    if dry_run:
        click.echo("Dry run -- no files will be written\n")

    written = 0
    unchanged = 0
    errors = 0

    for name, info in registry.items():
        try:
            result = _generate_fn(info.entity, output_dir=cfg.output_dir, dry_run=dry_run)
            if result.written:
                status = "written"
                written += 1
            else:
                status = "unchanged" if not dry_run else "dry-run"
                unchanged += 1
            click.echo(f"  {name:<20s} {result.output_path}   {status}")
        except Exception as e:
            click.echo(f"  {name:<20s} ERROR: {e}", err=True)
            errors += 1

    click.echo()
    if dry_run:
        click.echo(f"Dry run: {unchanged} would be generated")
    else:
        click.echo(f"Generated: {written} written, {unchanged} unchanged")

    if errors:
        click.echo(f"Errors: {errors}", err=True)
        sys.exit(1)


@main.command()
@click.option("--entity", "entity_name", default=None, help="Run a single entity by name.")
@click.option("--entities-dir", type=click.Path(), default=None, help="Override entities directory.")
@click.option("--data-dir", type=click.Path(), default=None, help="Override data directory.")
@click.option("--output-dir", type=click.Path(), default=None, help="Override generated output directory.")
@click.option("--backend", default=None, type=click.Choice(["duckdb", "bigquery"], case_sensitive=False), help="Override backend engine.")
@handle_errors
def run(entity_name: str | None, entities_dir: str | None, data_dir: str | None, output_dir: str | None, backend: str | None) -> None:
    """Execute the pipeline: discover, generate, transform, check."""
    from fyrnheim.config import resolve_config
    from fyrnheim.engine.registry import EntityRegistry
    from fyrnheim.engine.runner import run as engine_run, run_entity as engine_run_entity

    cfg = resolve_config(entities_dir=entities_dir, data_dir=data_dir, output_dir=output_dir, backend=backend)

    if not cfg.entities_dir.exists():
        click.echo(f"Error: Entities directory not found: {cfg.entities_dir}", err=True)
        raise SystemExit(1)

    registry = EntityRegistry()
    registry.discover(cfg.entities_dir)

    click.echo(f"Discovering entities... {len(registry)} found")

    if len(registry) == 0:
        click.echo("Nothing to run.")
        return

    if entity_name:
        info = registry.get(entity_name)
        if info is None:
            available = ", ".join(sorted(registry))
            click.echo(f"Error: Entity '{entity_name}' not found. Available: {available}", err=True)
            raise SystemExit(1)

        click.echo(f"Running {entity_name} on {cfg.backend}")
        click.echo()

        er = engine_run_entity(info.entity, cfg.data_dir, backend=cfg.backend, backend_config=cfg.backend_config, generated_dir=cfg.output_dir, output_backend=cfg.output_backend, output_config=cfg.output_config)
        _print_entity_result(er, registry)

        click.echo()
        click.echo(f"Done ({er.duration_seconds:.1f}s)")

        if er.status == "error":
            raise SystemExit(1)
        if er.quality_results and any(not qr.passed for qr in er.quality_results):
            raise SystemExit(2)
        return

    click.echo(f"Running on {cfg.backend}")
    click.echo()

    result = engine_run(cfg.entities_dir, cfg.data_dir, backend=cfg.backend, backend_config=cfg.backend_config, generated_dir=cfg.output_dir, output_backend=cfg.output_backend, output_config=cfg.output_config)

    for er in result.entities:
        _print_entity_result(er, registry)

    if result.pushed_tables:
        click.echo()
        click.echo(f"Pushing to {cfg.output_backend}...")
        for pt in result.pushed_tables:
            status = "ok" if pt.status == "ok" else f"ERROR: {pt.error}"
            click.echo(f"  {pt.table_name:<30s} {pt.row_count:>6d} rows  {status}")
        ok_count = sum(1 for pt in result.pushed_tables if pt.status == "ok")
        err_count = sum(1 for pt in result.pushed_tables if pt.status == "error")
        parts = [f"{ok_count} pushed"]
        if err_count:
            parts.append(f"{err_count} failed")
        click.echo(f"Push: {', '.join(parts)}")

    click.echo()
    _print_run_summary(result)

    exit_code = 0
    if result.error_count > 0:
        exit_code = 1
    elif _has_quality_failures(result.entities):
        exit_code = 2
    if exit_code:
        raise SystemExit(exit_code)


@main.command()
@click.option("--entity", "entity_name", default=None, help="Check a single entity by name.")
@click.option("--entities-dir", type=click.Path(), default=None, help="Override entities directory.")
@click.option("--output-dir", type=click.Path(), default=None, help="Override generated output directory.")
@click.option("--db-path", type=click.Path(), default=None, help="DuckDB database path.")
@handle_errors
def check(entity_name: str | None, entities_dir: str | None, output_dir: str | None, db_path: str | None) -> None:
    """Run quality checks against previously-executed entities."""
    from fyrnheim.config import resolve_config
    from fyrnheim.engine.executor import IbisExecutor
    from fyrnheim.engine.registry import EntityRegistry
    from fyrnheim.quality import QualityRunner

    cfg = resolve_config(entities_dir=entities_dir, output_dir=output_dir)

    if db_path is None:
        convention = cfg.project_root / "fyrnheim.duckdb"
        db_path = str(convention) if convention.exists() else ":memory:"

    if not cfg.entities_dir.exists():
        click.echo(f"Error: Entities directory not found: {cfg.entities_dir}", err=True)
        raise SystemExit(1)

    registry = EntityRegistry()
    registry.discover(cfg.entities_dir)

    if entity_name:
        info = registry.get(entity_name)
        if info is None:
            click.echo(f"Error: Entity '{entity_name}' not found.", err=True)
            raise SystemExit(1)
        targets = [(entity_name, info)]
    else:
        targets = list(registry.items())

    if not targets:
        click.echo("No entities found.")
        return

    executor = IbisExecutor.duckdb(db_path=db_path, generated_dir=cfg.output_dir)
    total_pass = total_fail = total_error = entities_checked = 0
    try:
        qr = QualityRunner(executor.connection)

        try:
            existing_tables = executor.connection.list_tables()
        except Exception:
            existing_tables = []

        for name, info in targets:
            e = info.entity
            if not e.quality or not e.quality.checks:
                click.echo(f"\n{name}: skipped (no quality checks defined)")
                continue

            table_name = _resolve_target_table(name, info.layers)
            if table_name not in existing_tables:
                click.echo(f"\n{name}: error (table '{table_name}' not found -- run 'fyr run' first)")
                total_error += 1
                continue

            try:
                result = qr.run_entity_checks(
                    entity_name=name,
                    quality_config=e.quality,
                    primary_key=e.quality.primary_key,
                    table_name=table_name,
                )
            except Exception as exc:
                click.echo(f"\n{name}: error ({exc})")
                total_error += 1
                continue

            entities_checked += 1
            click.echo(f"\n{name}:")
            for cr in result.results:
                if cr.passed:
                    click.echo(f"  {cr.check_name:<40s} pass")
                    total_pass += 1
                elif cr.error:
                    click.echo(f"  {cr.check_name:<40s} ERROR ({cr.error})")
                    total_error += 1
                else:
                    click.echo(f"  {cr.check_name:<40s} FAIL ({cr.failure_count} failures)")
                    total_fail += 1

        entity_word = "entity" if entities_checked == 1 else "entities"
        summary = f"\nChecks: {total_pass} passed, {total_fail} failed across {entities_checked} {entity_word}"
        if total_error > 0:
            error_word = "error" if total_error == 1 else "errors"
            summary += f" ({total_error} {error_word})"
        click.echo(summary)
    finally:
        executor.close()

    if total_error > 0:
        raise SystemExit(1)
    if total_fail > 0:
        raise SystemExit(2)


@main.command(name="list")
@click.option("--entities-dir", type=click.Path(), default=None, help="Override entities directory.")
@handle_errors
def list_cmd(entities_dir: str | None) -> None:
    """List discovered entities."""
    from fyrnheim.config import resolve_config
    from fyrnheim.engine.registry import EntityRegistry

    cfg = resolve_config(entities_dir=entities_dir)

    if not cfg.entities_dir.exists():
        click.echo(f"Error: Entities directory not found: {cfg.entities_dir}", err=True)
        raise SystemExit(1)

    registry = EntityRegistry()
    registry.discover(cfg.entities_dir)

    if len(registry) == 0:
        click.echo(f"No entities found in {cfg.entities_dir}")
        return

    for _name, info in registry.items():
        layers_str = ", ".join(info.layers)
        click.echo(f"  {info.name:<20s} {layers_str:<30s} {info.path}")

    click.echo(f"\n{len(registry)} entities found")
