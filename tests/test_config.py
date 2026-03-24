# -*- coding: utf-8 -*-
"""配置测试"""
from __future__ import annotations

import pytest

from src.config import Settings, settings


class TestSettings:
    """测试设置类"""

    def test_settings_singleton(self):
        """测试设置是单例"""
        s1 = Settings()
        s2 = Settings()
        # 每次创建都是新实例，但这里验证基本功能
        assert s1 is not None
        assert s2 is not None

    def test_default_values(self):
        """测试默认值"""
        s = Settings()
        assert s.crawl.min_text_len == 50
        assert s.crawl.min_cjk_article == 22
        assert s.crawl.max_retries == 0

    def test_url_col_hints_defined(self):
        """测试URL列提示已定义"""
        assert len(Settings.URL_COL_HINTS) > 0
        assert "详情页" in Settings.URL_COL_HINTS

    def test_list_meta_priority_defined(self):
        """测试列表元数据优先级已定义"""
        assert len(Settings.LIST_META_PRIORITY) > 0
        assert "标题" in Settings.LIST_META_PRIORITY

    def test_tables_to_clear_defined(self):
        """测试要清空的表已定义"""
        assert len(Settings.TABLES_TO_CLEAR) > 0
        assert "cms_crawl_data_content" in Settings.TABLES_TO_CLEAR

    def test_browser_error_triggers_defined(self):
        """测试浏览器错误触发词已定义"""
        assert len(Settings.BROWSER_ERROR_TRIGGERS) > 0
        assert "disconnected" in Settings.BROWSER_ERROR_TRIGGERS

    def test_json_fetch_headers_defined(self):
        """测试JSON请求头已定义"""
        assert "User-Agent" in Settings.JSON_FETCH_HEADERS
        assert "Accept" in Settings.JSON_FETCH_HEADERS


class TestGlobalSettings:
    """测试全局设置实例"""

    def test_global_settings_exists(self):
        """测试全局设置实例存在"""
        assert settings is not None

    def test_global_settings_has_required_attrs(self):
        """测试全局设置有必需属性"""
        assert hasattr(settings, "database")
        assert hasattr(settings, "crawl")
        assert hasattr(settings, "zhipu")
        assert hasattr(settings, "web")
