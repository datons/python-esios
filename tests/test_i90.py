"""Tests for I90 file processing — frequency detection and column normalisation."""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pytest

from esios.processing.i90 import I90Sheet, _any_value_greater_than_30


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_sheet() -> I90Sheet:
    """Return a bare I90Sheet instance backed by mocked objects."""
    wb = MagicMock()
    sheet = MagicMock()
    sheet.to_python.return_value = [[""]]
    wb.get_sheet_by_name.return_value = sheet
    return I90Sheet("test", wb, "I90DIA_20241001.xls", {})


def _full_nan_filler_columns(n_hours: int = 24) -> np.ndarray:
    """Build NaN-filler quarterly columns: [1, NaN, NaN, NaN, 2, NaN, …]."""
    cols: list = []
    for h in range(1, n_hours + 1):
        cols.append(h)
        cols.extend([np.nan, np.nan, np.nan])
    return np.array(cols, dtype=object)


def _full_hq_columns(n_hours: int = 24) -> np.ndarray:
    """Build explicit H-Q columns: ['1-1', '1-2', '1-3', '1-4', '2-1', …]."""
    return np.array(
        [f"{h}-{q}" for h in range(1, n_hours + 1) for q in range(1, 5)],
        dtype=object,
    )


def _mixed_hq_columns(n_hours: int = 24) -> np.ndarray:
    """First quarter is unlabelled ('1', '2', …); rest carry '-Q' suffix."""
    cols: list = []
    for h in range(1, n_hours + 1):
        cols.append(str(h))
        for q in range(2, 5):
            cols.append(f"{h}-{q}")
    return np.array(cols, dtype=object)


# ---------------------------------------------------------------------------
# _any_value_greater_than_30
# ---------------------------------------------------------------------------


class TestAnyValueGreaterThan30:
    def test_returns_true_for_value_above_30(self):
        assert _any_value_greater_than_30(np.array([1, 31, 24])) is True

    def test_returns_false_for_all_values_24_or_less(self):
        assert _any_value_greater_than_30(np.arange(1, 25)) is False

    def test_works_with_numpy_int64(self):
        """numpy 2.x broke isinstance(np.int64, int) — make sure we handle it."""
        arr = np.array([1, 2, 31, 96], dtype=np.int64)
        assert _any_value_greater_than_30(arr) is True

    def test_ignores_nan(self):
        arr = np.array([np.nan, 5.0, 20.0])
        assert _any_value_greater_than_30(arr) is False

    def test_sequential_quarterly_1_to_96(self):
        assert _any_value_greater_than_30(np.arange(1, 97)) is True


# ---------------------------------------------------------------------------
# _normalize_datetime_columns — hourly (unchanged behaviour)
# ---------------------------------------------------------------------------


class TestNormalizeHourly:
    def test_sequential_1_to_24(self):
        s = _make_sheet()
        cols = np.array([float(i) for i in range(1, 25)], dtype=object)
        result = s._normalize_datetime_columns(cols)
        assert list(result) == list(range(1, 25))
        assert not _any_value_greater_than_30(result)

    def test_n_columns_totals_is_2_when_no_nan(self):
        s = _make_sheet()
        cols = np.array([float(i) for i in range(1, 25)], dtype=object)
        s._normalize_datetime_columns(cols)
        assert s._n_columns_totals == 2


# ---------------------------------------------------------------------------
# _normalize_datetime_columns — quarterly (new behaviour)
# ---------------------------------------------------------------------------


class TestNormalizeQuarterlySequential:
    """Columns already in 1–96 sequential form (already worked before the fix)."""

    def test_sequential_1_to_96(self):
        s = _make_sheet()
        cols = np.array([float(i) for i in range(1, 97)], dtype=object)
        result = s._normalize_datetime_columns(cols)
        assert list(result) == list(range(1, 97))
        assert _any_value_greater_than_30(result)


class TestNormalizeQuarterlyHQFormat:
    """Columns in explicit 'H-Q' dash notation: '1-1', '1-2', …, '24-4'."""

    def test_full_day_96_periods(self):
        s = _make_sheet()
        result = s._normalize_datetime_columns(_full_hq_columns())
        assert len(result) == 96
        assert list(result) == list(range(1, 97))
        assert _any_value_greater_than_30(result)

    def test_time_deltas_are_correct(self):
        """period 1 → 0 min (00:00), period 96 → 1425 min (23:45)."""
        s = _make_sheet()
        result = s._normalize_datetime_columns(_full_hq_columns())
        time_deltas = (result - 1) * 15
        assert time_deltas[0] == 0
        assert time_deltas[-1] == 1425

    def test_mixed_hq_first_quarter_unlabelled(self):
        """'1', '1-2', '1-3', '1-4', '2', '2-2', … is treated the same."""
        s = _make_sheet()
        result = s._normalize_datetime_columns(_mixed_hq_columns())
        assert len(result) == 96
        assert list(result) == list(range(1, 97))
        assert _any_value_greater_than_30(result)

    def test_n_columns_totals_is_2_for_explicit_hq(self):
        s = _make_sheet()
        s._normalize_datetime_columns(_full_hq_columns())
        assert s._n_columns_totals == 2


class TestNormalizeQuarterlyNaNFiller:
    """Columns in NaN-filler form: [1, NaN, NaN, NaN, 2, NaN, …]."""

    def test_full_day_96_periods(self):
        s = _make_sheet()
        result = s._normalize_datetime_columns(_full_nan_filler_columns())
        assert len(result) == 96
        assert list(result) == list(range(1, 97))
        assert _any_value_greater_than_30(result)

    def test_time_deltas_are_correct(self):
        s = _make_sheet()
        result = s._normalize_datetime_columns(_full_nan_filler_columns())
        time_deltas = (result - 1) * 15
        assert time_deltas[0] == 0
        assert time_deltas[-1] == 1425

    def test_n_columns_totals_is_3_when_nan_present(self):
        s = _make_sheet()
        s._normalize_datetime_columns(_full_nan_filler_columns())
        assert s._n_columns_totals == 3
