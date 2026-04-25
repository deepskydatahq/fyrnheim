"""Smoke tests for fyrnheim package structure."""


def test_import_fyrnheim():
    """fyrnheim is importable."""
    import fyrnheim

    assert fyrnheim.__version__ == "0.13.0"


def test_import_subpackages():
    """All sub-packages are importable."""


def test_version_is_string():
    """Version is a proper string."""
    import fyrnheim

    assert isinstance(fyrnheim.__version__, str)
    parts = fyrnheim.__version__.split(".")
    assert len(parts) == 3, "Version should be semver (major.minor.patch)"
