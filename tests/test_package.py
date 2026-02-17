"""Smoke tests for typedata package structure."""


def test_import_typedata():
    """typedata is importable."""
    import typedata

    assert typedata.__version__ == "0.1.0"


def test_import_subpackages():
    """All sub-packages are importable."""


def test_version_is_string():
    """Version is a proper string."""
    import typedata

    assert isinstance(typedata.__version__, str)
    parts = typedata.__version__.split(".")
    assert len(parts) == 3, "Version should be semver (major.minor.patch)"
