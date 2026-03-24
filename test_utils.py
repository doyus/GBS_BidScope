# -*- coding: utf-8 -*-
"""
单元测试：工具模块 utils.py
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from datetime import datetime

import pandas as pd
import pytest

from utils import (
    _parse_id_cell,
    _series_has_urls,
    body_text_stats,
    calc_content_length,
    classify_error,
    detect_url_columns,
    domain_key_for_row,
    format_timestamp,
    get_db_connection,
    init_sqlite,
    migrate_db_schema,
    normalize_columns,
    parse_meta,
    parse_timestamp,
    pick_url,
    resolve_content_id,
    short_url,
    strip_tags,
    strip_tags_preview,
    url_fingerprint,
    valid_text_cjk_digit_alpha,
)


class TestTextProcessing:
    """文本处理函数测试"""

    def test_strip_tags(self):
        """测试去除 HTML 标签"""
        html = "<p>Hello <b>World</b></p>"
        result = strip_tags(html)
        assert result == "Hello World"

    def test_strip_tags_empty(self):
        """测试空输入"""
        assert strip_tags("") == ""
        assert strip_tags(None) == ""

    def test_strip_tags_preview(self):
        """测试带省略号的文本预览"""
        html = "<p>" + "非常长的文本内容" * 20 + "</p>"
        result = strip_tags_preview(html, max_len=10)
        assert len(result) <= 11  # 10 字符 + 省略号
        assert result.endswith("…")

    def test_valid_text_cjk_digit_alpha(self):
        """测试有效字符统计"""
        html = "<p>中文123ABC!@#</p>"
        count = valid_text_cjk_digit_alpha(html)
        assert count == 2 + 3 + 3  # 中文2 + 数字3 + 字母3

    def test_body_text_stats(self):
        """测试正文统计"""
        html = "<p>中文123ABC!@#</p>"
        stats = body_text_stats(html)
        assert stats["cn"] == 2
        assert stats["digit"] == 3
        assert stats["alpha"] == 3
        assert stats["total"] == 8

    def test_calc_content_length(self):
        """测试内容长度计算"""
        html = "<p>中文123ABC</p>"
        length = calc_content_length(html)
        assert length == 2 + 3 + 3

    def test_short_url(self):
        """测试 URL 缩短"""
        url = "https://www.example.com/very/long/path/that/needs/shortening"
        result = short_url(url, max_length=20)
        assert len(result) <= 21
        assert "…" in result

    def test_short_url_empty(self):
        """测试空 URL"""
        assert short_url("") == "—"
        assert short_url(None) == "—"


class TestUrlProcessing:
    """URL 处理函数测试"""

    def test_url_fingerprint(self):
        """测试 URL 指纹生成"""
        url1 = "HTTPS://www.Example.com/Path/"
        url2 = "https://www.example.com/Path"
        assert url_fingerprint(url1) == url_fingerprint(url2)

    def test_url_fingerprint_scheme(self):
        """测试协议处理"""
        url = "//example.com/path"
        result = url_fingerprint(url)
        assert result.startswith("https://")

    def test_url_fingerprint_empty(self):
        """测试空 URL"""
        assert url_fingerprint("") == ""

    def test_pick_url(self):
        """测试从行数据中选择 URL"""
        row = pd.Series(
            {
                "详情页": "https://example.com/detail",
                "其他列": "not a url",
            }
        )
        url_columns = ["详情页", "链接"]
        result = pick_url(row, url_columns)
        assert result == "https://example.com/detail"

    def test_pick_url_none(self):
        """测试没有找到 URL 的情况"""
        row = pd.Series({"其他列": "not a url"})
        url_columns = ["详情页"]
        assert pick_url(row, url_columns) is None

    def test_pick_url_relative(self):
        """测试相对 URL 补全"""
        row = pd.Series({"链接": "//example.com/path"})
        url_columns = ["链接"]
        result = pick_url(row, url_columns)
        assert result == "https://example.com/path"

    def test_domain_key_for_row(self):
        """测试域名提取"""
        row = pd.Series({"主域名": "www.example.com"})
        url = "https://test.com/page"
        result = domain_key_for_row(row, url)
        assert result == "example.com"

    def test_domain_key_for_row_from_url(self):
        """测试从 URL 提取域名"""
        row = pd.Series({})
        url = "https://www.test.example.com.cn/page"
        result = domain_key_for_row(row, url)
        assert result == "test.example.com.cn"


class TestMetaParsing:
    """元数据解析测试"""

    def test_parse_meta_valid(self):
        """测试有效 JSON 解析"""
        raw = '{"标题": "测试公告", "来源": "政府网站"}'
        result = parse_meta(raw)
        assert result["标题"] == "测试公告"
        assert result["来源"] == "政府网站"

    def test_parse_meta_invalid(self):
        """测试无效 JSON 处理"""
        raw = "not a json"
        result = parse_meta(raw)
        assert result == {}

    def test_parse_meta_none(self):
        """测试空输入"""
        assert parse_meta(None) == {}
        assert parse_meta("") == {}

    def test_parse_meta_non_dict(self):
        """测试非字典 JSON"""
        raw = '["item1", "item2"]'
        result = parse_meta(raw)
        assert result == {}


class TestExcelProcessing:
    """Excel 处理函数测试"""

    def test_normalize_columns(self):
        """测试列名规范化"""
        df = pd.DataFrame(
            {"  列 名 1  ": [1], "\xa0列名2": [2], "": [3], "  ": [4]}
        )
        result = normalize_columns(df)
        assert "列名1" in result.columns
        assert "列名2" in result.columns

    def test_series_has_urls(self):
        """测试 Series 是否包含 URL"""
        s = pd.Series(
            [
                "https://example.com",
                "http://test.com",
                "not a url",
                "https://another.com",
            ]
        )
        assert _series_has_urls(s, min_hits=2) is True

    def test_series_has_urls_false(self):
        """测试不包含 URL 的情况"""
        s = pd.Series(["text1", "text2", "text3"])
        assert _series_has_urls(s) is False

    def test_detect_url_columns(self):
        """测试 URL 列检测"""
        df = pd.DataFrame(
            {
                "标题": ["公告1", "公告2"],
                "详情页链接": ["https://a.com", "https://b.com"],
                "内容": ["text1", "text2"],
            }
        )
        result = detect_url_columns(df)
        assert "详情页链接" in result

    def test_detect_url_columns_force(self):
        """测试强制指定 URL 列"""
        df = pd.DataFrame(
            {"自定义链接": ["https://a.com", "https://b.com"], "其他列": [1, 2]}
        )
        result = detect_url_columns(df, force_names=["自定义链接"])
        assert "自定义链接" in result


class TestIdParsing:
    """ID 解析测试"""

    def test_parse_id_cell_valid(self):
        """测试有效 ID 解析"""
        assert _parse_id_cell("123") == 123
        assert _parse_id_cell(123.0) == 123
        assert _parse_id_cell("123.45") == 123

    def test_parse_id_cell_invalid(self):
        """测试无效 ID 解析"""
        assert _parse_id_cell("not a number") is None
        assert _parse_id_cell(None) is None
        assert _parse_id_cell("nan") is None

    def test_resolve_content_id(self):
        """测试内容 ID 解析"""
        row = pd.Series({"id": 123, "其他列": "value"})
        assert resolve_content_id(row) == 123

    def test_resolve_content_id_aus(self):
        """测试 aus_id 解析"""
        row = pd.Series({"aus_id": 456, "其他列": "value"})
        assert resolve_content_id(row) == 456

    def test_resolve_content_id_precedence(self):
        """测试 ID 优先级"""
        row = pd.Series({"id": 123, "aus_id": 456})
        assert resolve_content_id(row) == 123  # id 优先于 aus_id


class TestTimeProcessing:
    """时间处理测试"""

    def test_format_timestamp_seconds(self):
        """测试秒级时间戳格式化"""
        ts = datetime(2024, 1, 1, 12, 0, 0).timestamp()
        result = format_timestamp(ts)
        assert "2024-01-01" in result

    def test_format_timestamp_milliseconds(self):
        """测试毫秒级时间戳格式化"""
        ts = datetime(2024, 1, 1, 12, 0, 0).timestamp() * 1000
        result = format_timestamp(ts)
        assert "2024-01-01" in result

    def test_format_timestamp_invalid(self):
        """测试无效时间戳格式化"""
        assert format_timestamp("invalid") == "invalid"
        assert format_timestamp(None) == "—"

    def test_parse_timestamp_valid(self):
        """测试有效时间戳解析"""
        ts = datetime(2024, 1, 1, 12, 0, 0).timestamp()
        result = parse_timestamp(ts)
        assert result is not None
        assert result.year == 2024

    def test_parse_timestamp_invalid(self):
        """测试无效时间戳解析"""
        assert parse_timestamp("invalid") is None
        assert parse_timestamp(None) is None


class TestErrorClassification:
    """错误分类测试"""

    def test_classify_timeout(self):
        """测试超时错误分类"""
        error = "请求超时: connection timed out"
        assert classify_error(error) == "timeout"

    def test_classify_network(self):
        """测试网络错误分类"""
        error = "网络连接失败: 无法连接到服务器"
        assert classify_error(error) == "network"

    def test_classify_http_error(self):
        """测试 HTTP 错误分类"""
        error = "HTTP Error 404: Not Found"
        assert classify_error(error) == "http_error"

    def test_classify_other(self):
        """测试未知错误分类"""
        error = "未知错误类型"
        assert classify_error(error) == "other"


class TestDatabase:
    """数据库相关测试"""

    def test_init_sqlite(self):
        """测试数据库初始化"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            init_sqlite(db_path)

            # 验证表是否创建
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='cms_crawl_data_content'"
            )
            assert cursor.fetchone() is not None
            conn.close()
        finally:
            os.unlink(db_path)

    def test_get_db_connection(self):
        """测试获取数据库连接"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            init_sqlite(db_path)
            conn = get_db_connection(db_path)
            assert conn is not None
            assert conn.row_factory == sqlite3.Row
            conn.close()
        finally:
            os.unlink(db_path)

    def test_migrate_db_schema(self):
        """测试数据库迁移"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            # 创建基础表
            conn = sqlite3.connect(db_path)
            conn.execute(
                "CREATE TABLE IF NOT EXISTS cms_crawl_data_content (id INTEGER PRIMARY KEY)"
            )
            conn.commit()

            migrate_db_schema(conn)

            # 验证新列是否添加
            cursor = conn.execute("PRAGMA table_info(cms_crawl_data_content)")
            columns = {row[1] for row in cursor.fetchall()}
            assert "excel_meta" in columns
            assert "crawl_status" in columns
            conn.close()
        finally:
            os.unlink(db_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])