# -*- coding: utf-8 -*-
"""
config模块单元测试
"""
import os
from unittest.mock import patch


from config import (
    AppConfig,
    DatabaseConfig,
    ZhipuConfig,
    CrawlConfig,
    FlaskConfig,
    DisplayConfig,
    URLConfig,
    XPathConfig,
    config,
)


class TestDatabaseConfig:
    """数据库配置测试"""

    def test_default_db_path(self) -> None:
        """测试默认数据库路径"""
        db_config = DatabaseConfig()
        assert db_config.db_path.endswith("crawl_local.db")

    def test_custom_db_path(self) -> None:
        """测试自定义数据库路径"""
        with patch.dict(os.environ, {"SQLITE_DB": "/custom/path.db"}):
            db_config = DatabaseConfig()
            assert db_config.db_path == "/custom/path.db"

    def test_table_names(self) -> None:
        """测试表名常量"""
        db_config = DatabaseConfig()
        assert db_config.table_content == "cms_crawl_data_content"
        assert db_config.table_url_dedup == "crawl_url_dedup"


class TestZhipuConfig:
    """智谱AI配置测试"""

    def test_default_model(self) -> None:
        """测试默认模型"""
        zhipu_config = ZhipuConfig()
        assert zhipu_config.model == "GLM-4-Flash-250414"

    def test_custom_api_key(self) -> None:
        """测试自定义API Key"""
        with patch.dict(os.environ, {"ZHIPU_API_KEY": "test_key"}):
            zhipu_config = ZhipuConfig()
            assert zhipu_config.api_key == "test_key"

    def test_max_chars_limits(self) -> None:
        """测试字符限制"""
        zhipu_config = ZhipuConfig()
        assert zhipu_config.html_max_chars == 85000
        assert zhipu_config.json_max_chars == 120000


class TestCrawlConfig:
    """爬虫配置测试"""

    def test_default_values(self) -> None:
        """测试默认值"""
        crawl_config = CrawlConfig()
        assert crawl_config.min_text_length == 50
        assert crawl_config.min_cjk_article == 22
        assert crawl_config.max_retries == 0

    def test_custom_excel_path(self) -> None:
        """测试自定义Excel路径"""
        with patch.dict(os.environ, {"EXCEL_PATH": "/custom/excel.xlsx"}):
            crawl_config = CrawlConfig()
            assert crawl_config.excel_path == "/custom/excel.xlsx"


class TestFlaskConfig:
    """Flask配置测试"""

    def test_default_values(self) -> None:
        """测试默认值"""
        flask_config = FlaskConfig()
        assert flask_config.host == "127.0.0.1"
        assert flask_config.port == 5050
        assert flask_config.debug is True

    def test_custom_secret_key(self) -> None:
        """测试自定义密钥"""
        with patch.dict(os.environ, {"FLASK_SECRET_KEY": "secret123"}):
            flask_config = FlaskConfig()
            assert flask_config.secret_key == "secret123"


class TestDisplayConfig:
    """显示配置测试"""

    def test_list_meta_priority(self) -> None:
        """测试列表优先字段"""
        display_config = DisplayConfig()
        assert "标题" in display_config.list_meta_priority
        assert "主域名" in display_config.list_meta_priority

    def test_pagination_limits(self) -> None:
        """测试分页限制"""
        display_config = DisplayConfig()
        assert display_config.per_page_default == 30
        assert display_config.per_page_max == 100


class TestURLConfig:
    """URL配置测试"""

    def test_url_column_hints(self) -> None:
        """测试URL列提示"""
        url_config = URLConfig()
        assert "详情页" in url_config.url_column_hints
        assert "URL" in url_config.url_column_hints

    def test_json_fetch_headers(self) -> None:
        """测试JSON请求头"""
        url_config = URLConfig()
        assert "User-Agent" in url_config.json_fetch_headers
        assert "Accept" in url_config.json_fetch_headers


class TestXPathConfig:
    """XPath配置测试"""

    def test_static_xpaths(self) -> None:
        """测试静态XPath列表"""
        xpath_config = XPathConfig()
        assert len(xpath_config.static_xpaths) > 0
        assert "//article" in xpath_config.static_xpaths


class TestAppConfig:
    """应用配置测试"""

    def test_from_env(self) -> None:
        """测试从环境变量创建"""
        app_config = AppConfig.from_env()
        assert isinstance(app_config.database, DatabaseConfig)
        assert isinstance(app_config.zhipu, ZhipuConfig)

    def test_global_config(self) -> None:
        """测试全局配置实例"""
        assert isinstance(config, AppConfig)
        assert config.database is not None
        assert config.zhipu is not None
