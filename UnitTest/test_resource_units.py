"""Tests for Python-side workflow resource conversion."""
import pytest

from Yuki.utils.resource_units import memory_to_mb


@pytest.mark.parametrize(("quantity", "expected"), [
    ("256Mi", 256),
    ("512M", 512),
    ("1.5Gi", 1536),
    ("2GB", 2048),
    ("1 TiB", 1048576),
    ("500Ki", 1),
    (4096, 4096),
])
def test_memory_to_mb(quantity, expected):
    """Supported Celebi quantities convert to scheduler MB integers."""
    assert memory_to_mb(quantity) == expected


@pytest.mark.parametrize("quantity", [None, "", "invalid", "0Mi", "-1Gi"])
def test_invalid_memory_has_no_converted_value(quantity):
    """Invalid quantities remain unset so the runner can use its fallback."""
    assert memory_to_mb(quantity) is None
