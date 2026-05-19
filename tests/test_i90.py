"""Tests for I90 file processing — frequency detection and column normalisation."""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pytest

from esios.processing.i90 import (
    I90Sheet,
    _any_value_greater_than_30,
    _count_header_separators,
)


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


# ---------------------------------------------------------------------------
# _count_header_separators — header-label-based separator counting
#
# Headers below are calques of real REE I90 layouts. The counter walks back
# from idx_col_start, counting cells whose text equals a known separator
# label ('Cuarto de Hora del dia', 'Hora', 'Total', 'Indicadores', 'Hora del
# dia'), stopping at the first non-matching cell or NaN.
# ---------------------------------------------------------------------------


class TestCountHeaderSeparators:
    def test_returns_zero_when_no_separators(self):
        """If the cell right before the time block is a real index column."""
        row = np.array(["Redespacho", "Tipo", "1.0", "2.0"], dtype=object)
        assert _count_header_separators(row, idx_col_start=2) == 0

    def test_pre_mtu_rr_price_layout_returns_2(self):
        """I90DIA11 jun-2025: [Redespacho, Tipo, Cuarto de Hora del dia, Total, 1, …]."""
        row = np.array(
            ["Redespacho", "Tipo", "Cuarto de Hora del dia", "Total", "1.0", "2.0"],
            dtype=object,
        )
        assert _count_header_separators(row, idx_col_start=4) == 2

    def test_post_mtu_rr_price_layout_returns_1(self):
        """I90DIA11 nov-2025: 'Total' dropped → [Redespacho, Cuarto de Hora del dia, 1, …]."""
        row = np.array(
            ["Redespacho", "Cuarto de Hora del dia", "1.0", "2.0"],
            dtype=object,
        )
        assert _count_header_separators(row, idx_col_start=2) == 1

    def test_pre_2024_hourly_layout_returns_2(self):
        """I90DIA08 2014: [Redespacho, Tipo, …, Signo de Energía, Hora, Total, 00-01, …]."""
        row = np.array(
            [
                "Redespacho", "Tipo", "Sentido", "Unidad de Programación",
                "Tipo Oferta", "Tipo cálculo", "Tipo Restricción",
                "Signo de Energía", "Hora", "Total", "00-01", "01-02",
            ],
            dtype=object,
        )
        assert _count_header_separators(row, idx_col_start=10) == 2

    def test_post_mtu_rtr_price_layout_returns_1(self):
        """I90DIA10 nov-2025: 'Total' dropped + index reordered."""
        row = np.array(
            [
                "Redespacho", "Sentido", "Unidad de Programación", "Tipo Oferta",
                "Tipo cálculo", "Signo de Energía", "Cuarto de Hora del dia",
                "1.0", "2.0",
            ],
            dtype=object,
        )
        assert _count_header_separators(row, idx_col_start=7) == 1

    def test_double_index_date_row_with_nan_index_placeholders(self):
        """Double-header layout (e.g. I90DIA30 jun-2025): the date row carries
        the separator labels; positions under each real index column are NaN.
        The counter must stop at the first NaN (= start of the index zone).
        """
        row = np.array(
            [np.nan, np.nan, np.nan, np.nan,
             "Cuarto de Hora del dia", "Total", "1.0", "2.0"],
            dtype=object,
        )
        assert _count_header_separators(row, idx_col_start=6) == 2

    def test_double_index_post_mtu_dropped_total(self):
        """I90DIA30 nov-2025 (single-index path post-MTU)."""
        row = np.array(
            ["Redespacho", "Sentido", "Tipo QH",
             "Cuarto de Hora del dia", "1.0", "2.0"],
            dtype=object,
        )
        assert _count_header_separators(row, idx_col_start=4) == 1

    def test_match_is_case_insensitive(self):
        row = np.array(["Redespacho", "TOTAL", "1.0"], dtype=object)
        assert _count_header_separators(row, idx_col_start=2) == 1

    def test_match_is_whitespace_tolerant(self):
        row = np.array(["Redespacho", "  Total  ", "1.0"], dtype=object)
        assert _count_header_separators(row, idx_col_start=2) == 1

    def test_indicadores_counts_as_separator(self):
        """Variable-row layout: [Redespacho, Tipo, Indicadores, Precio Marginal …, …]."""
        row = np.array(
            ["Redespacho", "Tipo", "Indicadores", "Precio Marginal", "1.0"],
            dtype=object,
        )
        # idx_col_start = 4 → walk back from i=3
        # i=3 'Precio Marginal' is not a separator → stop, return 0
        assert _count_header_separators(row, idx_col_start=4) == 0
        # But when adjacent to time block it counts:
        row2 = np.array(["Redespacho", "Tipo", "Indicadores", "1.0"], dtype=object)
        assert _count_header_separators(row2, idx_col_start=3) == 1

    def test_unknown_label_immediately_after_index_stops_counter(self):
        """Non-separator labels do not get absorbed even when next to time block."""
        row = np.array(["Redespacho", "Foo Bar", "1.0"], dtype=object)
        assert _count_header_separators(row, idx_col_start=2) == 0

    def test_empty_string_breaks_counter(self):
        row = np.array(["Redespacho", "", "Total", "1.0"], dtype=object)
        # i=2 'Total' → sep, i=1 '' → break → returns 1
        assert _count_header_separators(row, idx_col_start=3) == 1


# ---------------------------------------------------------------------------
# _preprocess_*_index — verify the dynamic counter overrides the heuristic
# from _normalize_datetime_columns when slicing the index portion.
# ---------------------------------------------------------------------------


class TestPreprocessOverrideIntegration:
    def test_single_index_post_mtu_layout_keeps_redespacho_in_index(self):
        """I90DIA11 nov-2025 regression: pre-fix this consumed 'Redespacho' as
        a 'Total' placeholder and returned an empty DataFrame. Post-fix the
        index slice keeps it.
        """
        s = _make_sheet()
        # Plant a quarterly time block to match real shape (96 periods).
        time_block = np.array([float(i) for i in range(1, 97)], dtype=object)
        columns = np.concatenate([
            np.array(["Redespacho", "Cuarto de Hora del dia"], dtype=object),
            time_block,
        ])
        result = s._preprocess_single_index(idx_col_start=2, columns=columns)
        _, columns_index, _, _ = result
        assert list(columns_index) == ["Redespacho"]
        assert s._n_columns_totals == 1

    def test_single_index_pre_mtu_layout_still_drops_total(self):
        """I90DIA11 jun-2025 (and 2014-2024): two separators (Cuarto/Hora + Total)."""
        s = _make_sheet()
        time_block = np.array([float(i) for i in range(1, 97)], dtype=object)
        columns = np.concatenate([
            np.array(["Redespacho", "Tipo", "Cuarto de Hora del dia", "Total"],
                     dtype=object),
            time_block,
        ])
        result = s._preprocess_single_index(idx_col_start=4, columns=columns)
        _, columns_index, _, _ = result
        assert list(columns_index) == ["Redespacho", "Tipo"]
        assert s._n_columns_totals == 2
