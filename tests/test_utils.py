# -*- coding: utf-8 -*-
"""
utils模块单元测试
"""
import json
from datetime import datetime

import pandas as pd

from utils import (
    parse_meta,
    strip_tags,
    strip_tags_preview,
    count_valid_text,
    count_text_stats,
    format_timestamp,
    parse_timestamp,
    shorten_url,
    url_fingerprint,
    extract_domain,
    pandas_row_to_json,
    normalize_columns,
    parse_id_value,
    escape_html,
    build_status_where,
    get_order_by_sql,
)


class TestParseMeta:
    """parse_meta函数测试"""

    def test_valid_json(self) -> None:
        """测试有效JSON"""
        result = parse_meta('{"key": "value"}')
        assert result == {"key": "value"}

    def test_invalid_json(self) -> None:
        """测试无效JSON"""
        result = parse_meta("not json")
        assert result == {}

    def test_none_input(self) -> None:
        """测试None输入"""
        result = parse_meta(None)
        assert result == {}

    def test_empty_string(self) -> None:
        """测试空字符串"""
        result = parse_meta("")
        assert result == {}

    def test_json_array(self) -> None:
        """测试JSON数组"""
        result = parse_meta("[1, 2, 3]")
        assert result == {}


class TestStripTags:
    """strip_tags函数测试"""

    def test_basic_html(self) -> None:
        """测试基本HTML"""
        result = strip_tags("<p>Hello World</p>")
        assert result == "Hello World"

    def test_nested_tags(self) -> None:
        """测试嵌套标签"""
        result = strip_tags("<div><span>Nested</span></div>")
        assert result == "Nested"

    def test_none_input(self) -> None:
        """测试None输入"""
        result = strip_tags(None)
        assert result == ""

    def test_whitespace_handling(self) -> None:
        """测试空白处理"""
        result = strip_tags("<p>  Multiple   Spaces  </p>")
        assert "Multiple" in result


class TestStripTagsPreview:
    """strip_tags_preview函数测试"""

    def test_short_text(self) -> None:
        """测试短文本"""
        result = strip_tags_preview("<p>Short</p>", max_len=100)
        assert result == "Short"

    def test_long_text(self) -> None:
        """测试长文本"""
        long_text = "A" * 200
        result = strip_tags_preview(f"<p>{long_text}</p>", max_len=100)
        assert len(result) == 101
        assert result.endswith("…")

    def test_none_input(self) -> None:
        """测试None输入"""
        result = strip_tags_preview(None)
        assert result == ""


class TestCountValidText:
    """count_valid_text函数测试"""

    def test_chinese_text(self) -> None:
        """测试中文文本"""
        result = count_valid_text("中文测试内容")
        assert result == 6

    def test_mixed_content(self) -> None:
        """测试混合内容"""
        result = count_valid_text("中文ABC123")
        assert result == 8

    def test_html_tags_ignored(self) -> None:
        """测试HTML标签被忽略"""
        result = count_valid_text("<p>中文</p>")
        assert result == 2

    def test_none_input(self) -> None:
        """测试None输入"""
        result = count_valid_text(None)
        assert result == 0


class TestCountTextStats:
    """count_text_stats函数测试"""

    def test_basic_stats(self) -> None:
        """测试基本统计"""
        result = count_text_stats("中文ABC123")
        assert result["cn"] == 2
        assert result["alpha"] == 3
        assert result["digit"] == 3
        assert result["total"] == 8

    def test_none_input(self) -> None:
        """测试None输入"""
        result = count_text_stats(None)
        assert result["total"] == 0


class TestFormatTimestamp:
    """format_timestamp函数测试"""

    def test_valid_timestamp(self) -> None:
        """测试有效时间戳"""
        result = format_timestamp(1700000000)
        assert "-" in result
        assert ":" in result

    def test_millisecond_timestamp(self) -> None:
        """测试毫秒时间戳"""
        result = format_timestamp(1700000000000)
        assert "-" in result

    def test_none_input(self) -> None:
        """测试None输入"""
        result = format_timestamp(None)
        assert result == "—"


class TestParseTimestamp:
    """parse_timestamp函数测试"""

    def test_valid_timestamp(self) -> None:
        """测试有效时间戳"""
        result = parse_timestamp(1700000000)
        assert isinstance(result, datetime)

    def test_none_input(self) -> None:
        """测试None输入"""
        result = parse_timestamp(None)
        assert result is None


class TestShortenUrl:
    """shorten_url函数测试"""

    def test_short_url(self) -> None:
        """测试短URL"""
        result = shorten_url("https://example.com")
        assert result == "https://example.com"

    def test_long_url(self) -> None:
        """测试长URL"""
        long_url = "https://example.com/" + "a" * 100
        result = shorten_url(long_url, max_len=42)
        assert len(result) == 43
        assert result.endswith("…")

    def test_none_input(self) -> None:
        """测试None输入"""
        result = shorten_url(None)
        assert result == "—"


class TestUrlFingerprint:
    """url_fingerprint函数测试"""

    def test_basic_url(self) -> None:
        """测试基本URL"""
        result = url_fingerprint("https://Example.com/path/")
        assert "example.com" in result

    def test_protocol_relative(self) -> None:
        """测试协议相对URL"""
        result = url_fingerprint("//example.com/path")
        assert result.startswith("https://")

    def test_none_input(self) -> None:
        """测试None输入"""
        result = url_fingerprint(None)
        assert result == ""


class TestExtractDomain:
    """extract_domain函数测试"""

    def test_basic_domain(self) -> None:
        """测试基本域名"""
        result = extract_domain("https://www.example.com/path")
        assert result == "example.com"

    def test_empty_url(self) -> None:
        """测试空URL"""
        result = extract_domain("")
        assert result == "unknown"


class TestPandasRowToJson:
    """pandas_row_to_json函数测试"""

    def test_basic_row(self) -> None:
        """测试基本行"""
        row = pd.Series({"name": "test", "value": 123})
        result = pandas_row_to_json(row)
        data = json.loads(result)
        assert data["name"] == "test"
        assert data["value"] == 123

    def test_nan_value(self) -> None:
        """测试NaN值"""
        row = pd.Series({"name": "test", "value": float("nan")})
        result = pandas_row_to_json(row)
        data = json.loads(result)
        assert data["value"] is None


class TestNormalizeColumns:
    """normalize_columns函数测试"""

    def test_basic_normalization(self) -> None:
        """测试基本规范化"""
        df = pd.DataFrame({"  Name  ": [1], "Value\t": [2]})
        result = normalize_columns(df)
        assert "Name" in result.columns
        assert "Value" in result.columns


class TestParseIdValue:
    """parse_id_value函数测试"""

    def test_valid_integer(self) -> None:
        """测试有效整数"""
        result = parse_id_value(123)
        assert result == 123

    def test_valid_string(self) -> None:
        """测试有效字符串"""
        result = parse_id_value("456")
        assert result == 456

    def test_none_input(self) -> None:
        """测试None输入"""
        result = parse_id_value(None)
        assert result is None

    def test_nan_input(self) -> None:
        """测试NaN输入"""
        result = parse_id_value(float("nan"))
        assert result is None


class TestEscapeHtml:
    """escape_html函数测试"""

    def test_basic_escape(self) -> None:
        """测试基本转义"""
        result = escape_html("<script>")
        assert "<" not in result
        assert "&lt;" in result

    def test_none_input(self) -> None:
        """测试None输入"""
        result = escape_html(None)
        assert result == ""


class TestBuildStatusWhere:
    """build_status_where函数测试"""

    def test_all_status(self) -> None:
        """测试全部状态"""
        where, params = build_status_where("all")
        assert "1=1" in where

    def test_ok_status(self) -> None:
        """测试成功状态"""
        where, params = build_status_where("ok")
        assert "crawl_status" in where.lower()

    def test_failed_status(self) -> None:
        """测试失败状态"""
        where, params = build_status_where("failed")
        assert "failed" in where


class TestGetOrderBySql:
    """get_order_by_sql函数测试"""

    def test_returns_string(self) -> None:
        """测试返回字符串"""
        result = get_order_by_sql()
        assert isinstance(result, str)
        assert "DESC" in result
