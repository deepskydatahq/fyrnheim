"""HTML documentation generator that embeds catalog JSON into the template."""

from __future__ import annotations

import importlib.resources
import json
from pathlib import Path
from typing import Any

_DOCS_PKG = "fyrnheim.docs"
_PLACEHOLDER = "/*CATALOG_JSON*/"


def generate_docs(catalog: dict[str, Any], output_dir: str | Path) -> Path:
    """Generate a self-contained HTML documentation site.

    Reads the template.html, replaces the catalog placeholder with the
    serialized catalog JSON, and writes index.html to output_dir.

    Args:
        catalog: The catalog dict (from build_catalog).
        output_dir: Directory to write index.html into. Created if missing.

    Returns:
        Path to the written index.html file.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Read the template
    template_resource = importlib.resources.files(_DOCS_PKG).joinpath("template.html")
    with importlib.resources.as_file(template_resource) as template_path:
        template = template_path.read_text(encoding="utf-8")

    # Embed catalog JSON
    catalog_json = json.dumps(catalog, indent=2, default=str)
    html = template.replace(_PLACEHOLDER, catalog_json)

    # Write output
    index_path = output_path / "index.html"
    index_path.write_text(html, encoding="utf-8")

    return index_path
