"""Tests for the HTML template file."""

from pathlib import Path


TEMPLATE_PATH = Path(__file__).parent.parent / "src" / "fyrnheim" / "docs" / "template.html"


def test_template_file_exists() -> None:
    """Template file must exist at expected path."""
    assert TEMPLATE_PATH.exists()


def test_template_has_catalog_placeholder() -> None:
    """Template must contain the placeholder for catalog data injection."""
    content = TEMPLATE_PATH.read_text()
    assert "/*CATALOG_JSON*/" in content


def test_template_has_dagre_d3() -> None:
    """Template must include dagre-d3 JS for DAG rendering."""
    content = TEMPLATE_PATH.read_text()
    assert "dagre-d3" in content


def test_template_has_sidebar() -> None:
    """Template must have a sidebar element."""
    content = TEMPLATE_PATH.read_text()
    assert 'id="sidebar"' in content
    assert 'id="entity-list"' in content


def test_template_has_dag_view() -> None:
    """Template must have a DAG view section."""
    content = TEMPLATE_PATH.read_text()
    assert 'id="dag-view"' in content


def test_template_has_entity_detail_section() -> None:
    """Template must have an entity detail section."""
    content = TEMPLATE_PATH.read_text()
    assert 'id="detail-view"' in content


def test_template_is_complete_html() -> None:
    """Template must be a complete HTML document."""
    content = TEMPLATE_PATH.read_text()
    assert "<!DOCTYPE html>" in content
    assert "<html" in content
    assert "</html>" in content
    assert "<head>" in content
    assert "</head>" in content
    assert "<body>" in content
    assert "</body>" in content
