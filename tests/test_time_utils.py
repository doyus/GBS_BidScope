# -*- coding: utf-8 -*-
"""时间工具测试"""
from __future__ import annotations

from datetime import datetime

import pytest

from src.utils.time_utils import (
    format_date,
    format_timestamp,
    get_date_range,
    parse_timestamp,
)


class TestParseTimestamp:
    """测试时间戳解析"""

    def test_parses_seconds_timestamp(self):
        ts = 1609459200.0  # 2021-01-01 00:00:00 UTC
        result = parse_timestamp(ts)
        assert isinstance(result, datetime)

    def test_parses_milliseconds_timestamp(self):
        ts = 1609459200000.0  # 毫秒时间戳
        result = parse_timestamp(ts)
        assert isinstance(result, datetime)

    def test_parses_string_timestamp(self):
        ts = "1609459200"
        result = parse_timestamp(ts)
        assert isinstance(result, datetime)

    def test_none_returns_none(self):
        assert parse_timestamp(None) is None

    def test_invalid_returns_none(self):
        assert parse_timestamp("invalid") is None


class TestFormatTimestamp:
    """测试时间戳格式化"""

    def test_formats_valid_timestamp(self):
        ts = 1609459200.0
        result = format_timestamp(ts)
        assert isinstance(result, str)
        assert len(result) > 0

    def test_none_returns_dash(self):
        assert format_timestamp(None) == "—"

    def test_invalid_returns_dash(self):
        result = format_timestamp("invalid")
        assert result == "—"


class TestFormatDate:
    """测试日期格式化"""

    def test_formats_valid_timestamp(self):
        ts = 1609459200.0
        result = format_date(ts)
        assert isinstance(result, str)
        assert "-" in result

    def test_none_returns_none(self):
        assert format_date(None) is None


class TestGetDateRange:
    """测试日期范围"""

    def test_returns_min_max_dates(self):
        timestamps = [1609459200.0, 1609545600.0, 1609632000.0]
        result = get_date_range(timestamps)
        assert "start" in result
        assert "end" in result
        assert result["start"] <= result["end"]

    def test_empty_list_returns_none(self):
        result = get_date_range([])
        assert result["start"] is None
        assert result["end"] is None

    def test_with_none_values(self):
        timestamps = [1609459200.0, None, 1609632000.0]
        result = get_date_range(timestamps)
        assert result["start"] is not None
        assert result["end"] is not None
