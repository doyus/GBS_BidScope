# -*- coding: utf-8 -*-
"""
单元测试：配置模块 config.py
"""
from __future__ import annotations

import os

import pytest

import config


class TestConfig:
    """配置模块测试"""

    def test_base_path_config(self):
        """测试基础路径配置"""
        assert hasattr(config, "BASE_DIR")
        assert os.path.exists(config.BASE_DIR)
        assert hasattr(config, "EXCEL_PATH")
        assert hasattr(config, "SQLITE_DB")
        assert hasattr(config, "FLASK_SECRET_KEY")
        assert hasattr(config, "PORT")
        assert isinstance(config.PORT, int)

    def test_zhipu_config(self):
        """测试智谱 AI 配置"""
        assert hasattr(config, "ZHIPU_API_KEY")
        assert hasattr(config, "ZHIPU_MODEL")
        assert isinstance(config.ZHIPU_API_KEY, str)
        assert isinstance(config.ZHIPU_MODEL, str)

    def test_error_patterns(self):
        """测试错误模式配置"""
        assert hasattr(config, "ERROR_PATTERNS")
        assert isinstance(config.ERROR_PATTERNS, dict)
        assert "timeout" in config.ERROR_PATTERNS
        assert "network" in config.ERROR_PATTERNS
        assert "http_error" in config.ERROR_PATTERNS

    def test_db_tables_config(self):
        """测试数据库表名配置"""
        assert hasattr(config, "DB_TABLES")
        assert isinstance(config.DB_TABLES, dict)
        assert "content" in config.DB_TABLES
        assert "xpath" in config.DB_TABLES

    def test_url_column_hints(self):
        """测试 URL 列提示配置"""
        assert hasattr(config, "URL_COL_HINTS")
        assert isinstance(config.URL_COL_HINTS, tuple)
        assert len(config.URL_COL_HINTS) > 0

    def test_crawl_constants(self):
        """测试抓取相关常量"""
        assert hasattr(config, "MIN_TEXT_LEN")
        assert hasattr(config, "MIN_CJK_ARTICLE")
        assert hasattr(config, "MAX_CRAWL_RETRIES_DEFAULT")
        assert isinstance(config.MIN_TEXT_LEN, int)

    def test_llm_limits(self):
        """测试 LLM 限制配置"""
        assert hasattr(config, "LLM_HTML_MAX")
        assert hasattr(config, "LLM_JSON_MAX")
        assert isinstance(config.LLM_HTML_MAX, int)

    def test_http_headers(self):
        """测试 HTTP 请求头配置"""
        assert hasattr(config, "JSON_FETCH_HEADERS")
        assert isinstance(config.JSON_FETCH_HEADERS, dict)
        assert "User-Agent" in config.JSON_FETCH_HEADERS

    def test_static_xpaths(self):
        """测试静态 XPath 配置"""
        assert hasattr(config, "STATIC_XPATHS")
        assert isinstance(config.STATIC_XPATHS, list)
        assert len(config.STATIC_XPATHS) > 0

    def test_tables_to_clear(self):
        """测试需要清空的表配置"""
        assert hasattr(config, "TABLES_TO_CLEAR")
        assert isinstance(config.TABLES_TO_CLEAR, tuple)
        assert len(config.TABLES_TO_CLEAR) > 0

    def test_get_db_path_function(self):
        """测试获取数据库路径函数"""
        assert hasattr(config, "get_db_path")
        assert callable(config.get_db_path)
        db_path = config.get_db_path()
        assert isinstance(db_path, str)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])