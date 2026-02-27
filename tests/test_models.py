"""Tests for Pydantic models."""

from esios.models.indicator import Indicator, IndicatorValue
from esios.models.archive import Archive, ArchiveDownload
from esios.models.offer_indicator import OfferIndicator


class TestIndicator:
    def test_from_api(self):
        data = {
            "id": 600,
            "name": "PVPC",
            "short_name": "PVPC",
            "description": "Precio voluntario",
        }
        ind = Indicator.from_api(data)
        assert ind.id == 600
        assert ind.name == "PVPC"
        assert ind.raw == data

    def test_extra_fields_allowed(self):
        data = {"id": 1, "name": "Test", "unknown_field": "ok"}
        ind = Indicator.from_api(data)
        assert ind.id == 1


class TestIndicatorValue:
    def test_basic(self):
        val = IndicatorValue(
            value=120.5,
            datetime="2025-01-01T00:00:00Z",
        )
        assert val.value == 120.5


class TestArchive:
    def test_from_api_with_download(self):
        data = {
            "id": 1,
            "name": "I90DIA",
            "horizon": "D",
            "archive_type": "xls",
            "download": {"name": "I90DIA_20250101", "url": "/download/1"},
        }
        arch = Archive.from_api(data)
        assert arch.id == 1
        assert arch.download.url == "/download/1"

    def test_from_api_no_download(self):
        data = {"id": 2, "name": "Test", "horizon": "M", "archive_type": "zip"}
        arch = Archive.from_api(data)
        assert arch.download is None


class TestOfferIndicator:
    def test_from_api(self):
        data = {"id": 100, "name": "Offer Test", "short_name": "OT", "description": ""}
        oi = OfferIndicator.from_api(data)
        assert oi.id == 100
        assert oi.name == "Offer Test"
