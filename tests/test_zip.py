"""Tests for ZipExtractor."""

import zipfile
from io import BytesIO
from pathlib import Path

import pytest

from esios.processing.zip import ZipExtractor


@pytest.fixture
def simple_zip() -> bytes:
    """Create an in-memory zip with one text file."""
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("test.txt", "hello world")
    return buf.getvalue()


@pytest.fixture
def nested_zip() -> bytes:
    """Create a zip containing another zip."""
    inner = BytesIO()
    with zipfile.ZipFile(inner, "w") as zf:
        zf.writestr("inner.txt", "nested content")
    inner_bytes = inner.getvalue()

    outer = BytesIO()
    with zipfile.ZipFile(outer, "w") as zf:
        zf.writestr("inner.zip", inner_bytes)
    return outer.getvalue()


def test_extract_simple(tmp_path, simple_zip):
    zx = ZipExtractor(simple_zip, tmp_path / "out")
    zx.unzip()
    assert (tmp_path / "out" / "test.txt").read_text() == "hello world"


def test_extract_nested(tmp_path, nested_zip):
    zx = ZipExtractor(nested_zip, tmp_path / "out")
    zx.unzip()
    assert (tmp_path / "out" / "inner" / "inner.txt").read_text() == "nested content"
