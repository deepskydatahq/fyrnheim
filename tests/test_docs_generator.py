"""Tests for the HTML documentation generator."""

import json
from pathlib import Path

from fyrnheim.docs.generator import generate_docs


def test_generate_docs_writes_index_html(tmp_path: Path) -> None:
    """generate_docs writes output_dir/index.html."""
    catalog = {"entities": [], "metadata": {"entity_count": 0}}
    result = generate_docs(catalog, tmp_path / "docs")
    assert result == tmp_path / "docs" / "index.html"
    assert result.exists()


def test_generate_docs_embeds_catalog_json(tmp_path: Path) -> None:
    """The written HTML contains the catalog data as embedded JSON."""
    catalog = {
        "entities": [
            {"name": "users", "source_type": "table", "fields": [], "dependencies": []}
        ],
        "metadata": {"entity_count": 1},
    }
    result = generate_docs(catalog, tmp_path / "docs")
    html = result.read_text()
    # The catalog JSON should be embedded in the HTML
    assert '"users"' in html
    assert '"entity_count": 1' in html
    # Should not still have the placeholder
    assert "/*CATALOG_JSON*/" not in html


def test_generate_docs_creates_output_dir(tmp_path: Path) -> None:
    """generate_docs creates the output directory if it doesn't exist."""
    nested = tmp_path / "a" / "b" / "c"
    assert not nested.exists()
    result = generate_docs({"entities": [], "metadata": {}}, nested)
    assert nested.exists()
    assert result.exists()


def test_generate_docs_empty_catalog_produces_valid_html(tmp_path: Path) -> None:
    """generate_docs with empty catalog produces valid HTML."""
    catalog: dict = {"entities": [], "metadata": {}}
    result = generate_docs(catalog, tmp_path)
    html = result.read_text()
    assert "<!DOCTYPE html>" in html
    assert "</html>" in html
    # The embedded JSON should parse correctly
    # Extract the JSON between 'const CATALOG = ' and ';'
    start = html.index("const CATALOG = ") + len("const CATALOG = ")
    end = html.index(";", start)
    embedded = html[start:end]
    parsed = json.loads(embedded)
    assert parsed == catalog


def test_generate_docs_returns_path(tmp_path: Path) -> None:
    """generate_docs returns the path to the written file."""
    result = generate_docs({"entities": [], "metadata": {}}, tmp_path)
    assert isinstance(result, Path)
    assert result.name == "index.html"


def test_generate_docs_string_output_dir(tmp_path: Path) -> None:
    """generate_docs accepts string output_dir."""
    result = generate_docs({"entities": [], "metadata": {}}, str(tmp_path / "out"))
    assert result.exists()


def test_generate_docs_overwrites_existing(tmp_path: Path) -> None:
    """generate_docs overwrites an existing index.html."""
    catalog1 = {"entities": [], "metadata": {"version": "1"}}
    catalog2 = {"entities": [], "metadata": {"version": "2"}}
    generate_docs(catalog1, tmp_path)
    result = generate_docs(catalog2, tmp_path)
    html = result.read_text()
    assert '"version": "2"' in html
