# -*- coding: utf-8 -*-
"""URL工具测试"""
from __future__ import annotations

import pandas as pd
import pytest

from src.utils.url_utils import (
    extract_domain,
    is_valid_url,
    pick_url_from_row,
    url_fingerprint,
)


class TestUrlFingerprint:
    """测试URL指纹生成"""

    def test_normalizes_http_url(self):
        url = "http://Example.COM/path/"
        result = url_fingerprint(url)
        assert result == "http://example.com/path"

    def test_normalizes_https_url(self):
        url = "https://Example.COM/path"
        result = url_fingerprint(url)
        assert result == "https://example.com/path"

    def test_handles_protocol_relative(self):
        url = "//example.com/path"
        result = url_fingerprint(url)
        assert result == "https://example.com/path"

    def test_preserves_query_string(self):
        url = "https://example.com/path?key=value"
        result = url_fingerprint(url)
        assert "key=value" in result

    def test_empty_returns_empty(self):
        assert url_fingerprint("") == ""

    def test_none_returns_empty(self):
        assert url_fingerprint(None) == ""


class TestExtractDomain:
    """测试域名提取"""

    def test_extracts_from_http(self):
        assert extract_domain("http://example.com/path") == "example.com"

    def test_extracts_from_https(self):
        assert extract_domain("https://example.com/path") == "example.com"

    def test_removes_www(self):
        assert extract_domain("https://www.example.com") == "example.com"

    def test_unknown_returns_unknown(self):
        assert extract_domain("") == "unknown"


class TestPickUrlFromRow:
    """测试从行中选取URL"""

    def test_picks_first_valid_url(self):
        row = pd.Series(
            {
                "url1": "https://example1.com",
                "url2": "https://example2.com",
            }
        )
        result = pick_url_from_row(row, ["url1", "url2"])
        assert result == "https://example1.com"

    def test_skips_invalid_urls(self):
        row = pd.Series(
            {
                "url1": "not a url",
                "url2": "https://example.com",
            }
        )
        result = pick_url_from_row(row, ["url1", "url2"])
        assert result == "https://example.com"

    def test_handles_protocol_relative(self):
        row = pd.Series({"url": "//example.com"})
        result = pick_url_from_row(row, ["url"])
        assert result == "https://example.com"

    def test_returns_none_if_no_valid(self):
        row = pd.Series({"url": "not a url"})
        result = pick_url_from_row(row, ["url"])
        assert result is None


class TestIsValidUrl:
    """测试URL有效性检查"""

    def test_valid_http(self):
        assert is_valid_url("http://example.com")

    def test_valid_https(self):
        assert is_valid_url("https://example.com")

    def test_invalid_url(self):
        assert not is_valid_url("not a url")

    def test_empty_string(self):
        assert not is_valid_url("")
