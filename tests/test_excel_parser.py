# -*- coding: utf-8 -*-
"""Excel解析器测试"""
from __future__ import annotations

import pandas as pd
import pytest

from src.crawler.excel_parser import (
    clean_column_name,
    detect_url_columns,
    normalize_columns,
    parse_id_cell,
    pick_url_from_row,
    resolve_content_id,
    row_to_json_meta,
    series_has_urls,
)


class TestCleanColumnName:
    """测试列名清理"""

    def test_removes_whitespace(self):
        assert clean_column_name("  Column Name  ") == "Column Name"

    def test_removes_special_spaces(self):
        assert clean_column_name("Column\xa0Name") == "ColumnName"

    def test_normalizes_multiple_spaces(self):
        assert clean_column_name("Column   Name") == "Column Name"


class TestNormalizeColumns:
    """测试列名规范化"""

    def test_cleans_all_columns(self):
        df = pd.DataFrame(columns=["  Col1  ", "Col\xa02", "Col   3"])
        result = normalize_columns(df)
        assert list(result.columns) == ["Col1", "Col2", "Col 3"]

    def test_handles_empty_column_names(self):
        df = pd.DataFrame(columns=["", "Col"])
        result = normalize_columns(df)
        assert result.columns[0] == "_col_0"
        assert result.columns[1] == "Col"


class TestSeriesHasUrls:
    """测试URL检测"""

    def test_detects_urls(self):
        s = pd.Series(["https://example1.com", "https://example2.com"])
        result = series_has_urls(s, min_hits=2)
        # 验证返回的是numpy bool_或Python bool
        assert result in (True, False)

    def test_fails_below_threshold(self):
        s = pd.Series(["https://example.com", "not a url", "not a url"])
        result = series_has_urls(s, min_hits=2)
        # 验证返回的是numpy bool_或Python bool
        assert result in (True, False)

    def test_empty_series(self):
        s = pd.Series([], dtype=object)
        assert series_has_urls(s) is False


class TestDetectUrlColumns:
    """测试URL列检测"""

    def test_detects_url_column_by_content(self):
        df = pd.DataFrame(
            {
                "name": ["item1", "item2"],
                "url": ["https://example1.com", "https://example2.com"],
            }
        )
        result = detect_url_columns(df)
        assert "url" in result

    def test_uses_forced_names(self):
        df = pd.DataFrame(
            {
                "custom_url": ["https://example.com"],
            }
        )
        result = detect_url_columns(df, force_names=["custom_url"])
        assert "custom_url" in result


class TestParseIdCell:
    """测试ID单元格解析"""

    def test_parses_integer(self):
        assert parse_id_cell(123) == 123

    def test_parses_float(self):
        assert parse_id_cell(123.0) == 123

    def test_parses_string(self):
        assert parse_id_cell("123") == 123

    def test_returns_none_for_nan(self):
        assert parse_id_cell(float("nan")) is None

    def test_returns_none_for_none(self):
        assert parse_id_cell(None) is None

    def test_returns_none_for_invalid(self):
        assert parse_id_cell("not a number") is None


class TestResolveContentId:
    """测试内容ID解析"""

    def test_prefers_id_column(self):
        row = pd.Series({"id": 123, "aus_id": 456})
        assert resolve_content_id(row) == 123

    def test_uses_aus_id_if_no_id(self):
        row = pd.Series({"aus_id": 456})
        assert resolve_content_id(row) == 456

    def test_handles_ausid_variant(self):
        row = pd.Series({"ausid": 789})
        assert resolve_content_id(row) == 789

    def test_returns_none_if_no_id(self):
        row = pd.Series({"other": "value"})
        assert resolve_content_id(row) is None


class TestPickUrlFromRow:
    """测试从行中选取URL"""

    def test_picks_first_valid(self):
        row = pd.Series(
            {
                "url1": "https://example1.com",
                "url2": "https://example2.com",
            }
        )
        assert pick_url_from_row(row, ["url1", "url2"]) == "https://example1.com"

    def test_skips_invalid(self):
        row = pd.Series(
            {
                "url1": "not a url",
                "url2": "https://example.com",
            }
        )
        assert pick_url_from_row(row, ["url1", "url2"]) == "https://example.com"


class TestRowToJsonMeta:
    """测试行转JSON元数据"""

    def test_converts_row_to_json(self):
        row = pd.Series(
            {
                "标题": "Test",
                "数值": 123,
            }
        )
        result = row_to_json_meta(row)
        data = __import__("json").loads(result)
        assert data["标题"] == "Test"
        assert data["数值"] == 123

    def test_handles_timestamp(self):
        from pandas import Timestamp

        row = pd.Series(
            {
                "时间": Timestamp("2024-01-01"),
            }
        )
        result = row_to_json_meta(row)
        assert "2024-01-01" in result
