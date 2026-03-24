# -*- coding: utf-8 -*-
"""
项目配置模块 - 集中管理所有配置项，消除硬编码
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Final

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


@dataclass(frozen=True)
class DatabaseConfig:
    """数据库配置"""

    db_path: str = field(
        default_factory=lambda: os.environ.get(
            "SQLITE_DB", os.path.join(os.path.dirname(__file__), "crawl_local.db")
        )
    )
    table_content: Final[str] = "cms_crawl_data_content"
    table_url_dedup: Final[str] = "crawl_url_dedup"
    table_domain_xpath: Final[str] = "domain_learned_xpath"
    table_domain_json_path: Final[str] = "domain_json_html_path"


@dataclass(frozen=True)
class ZhipuConfig:
    """智谱AI配置"""

    api_key: str = field(default_factory=lambda: os.environ.get("ZHIPU_API_KEY", ""))
    model: str = field(
        default_factory=lambda: os.environ.get("ZHIPU_MODEL", "GLM-4-Flash-250414")
    )
    html_max_chars: Final[int] = 85000
    json_max_chars: Final[int] = 120000
    max_tokens: Final[int] = 100


@dataclass(frozen=True)
class CrawlConfig:
    """爬虫配置"""

    excel_path: str = field(
        default_factory=lambda: os.environ.get(
            "EXCEL_PATH", os.path.join(os.path.dirname(__file__), "1.xlsx")
        )
    )
    min_text_length: Final[int] = 50
    min_cjk_article: Final[int] = 22
    min_valid_loose: Final[int] = 28
    max_retries: Final[int] = 0
    attempt_wait_sec: Final[float] = 1.0
    round_interval_sec: Final[float] = 10.0
    page_timeout: Final[float] = 5.0
    scroll_pause: Final[float] = 0.55
    scroll_max_rounds: Final[int] = 40
    max_browser_restart: Final[int] = 15


@dataclass(frozen=True)
class FlaskConfig:
    """Flask配置"""

    secret_key: str = field(
        default_factory=lambda: os.environ.get(
            "FLASK_SECRET_KEY", "local-viewer-dev-key"
        )
    )
    host: Final[str] = "127.0.0.1"
    port: Final[int] = 5050
    debug: Final[bool] = True


@dataclass(frozen=True)
class DisplayConfig:
    """显示配置"""

    list_meta_priority: tuple[str, ...] = (
        "标题",
        "主域名",
        "来源",
        "aus_id",
        "详情页地址_链接",
        "Comment地址",
        "详情页",
        "Content地址",
        "源域名",
    )
    url_short_length: Final[int] = 42
    preview_max_length: Final[int] = 100
    per_page_default: Final[int] = 30
    per_page_max: Final[int] = 100
    max_display_columns: Final[int] = 8


@dataclass(frozen=True)
class URLConfig:
    """URL相关配置"""

    url_column_hints: tuple[str, ...] = (
        "详情页",
        "详情页链接",
        "详情链接",
        "详情地址",
        "Content地址",
        "content地址",
        "content",
        "链接",
        "URL",
        "url",
        "link",
        "页面地址",
        "网页地址",
        "href",
        "源链接",
    )
    json_fetch_headers: dict[str, str] = field(
        default_factory=lambda: {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
        }
    )


@dataclass(frozen=True)
class XPathConfig:
    """XPath配置"""

    static_xpaths: tuple[str, ...] = (
        "//article",
        "//*[@id='content' or @id='Content' or @id='mainContent' or @id='articleContent']",
        "//*[@class='article-content' or @class='article_content' or contains(@class,'article-detail')]",
        "//*[@class='detail' or contains(@class,'detail-content') or contains(@class,'detail_content')]",
        "//div[contains(@class,'zw') or contains(@class,'news_content') or contains(@class,'news-content')]",
        "//div[contains(@class,'ggnr') or contains(@class,'announce') or contains(@class,'notice-body')]",
        "//div[contains(@class,'TRS_Editor') or @id='TRS_AUTOADD']",
        "//div[contains(@class,'main') and string-length(normalize-space(.))>500]",
        "//div[contains(@class,'article')]",
        "//td[contains(@class,'content') or contains(@class,'article')]",
    )


@dataclass
class AppConfig:
    """应用总配置"""

    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    zhipu: ZhipuConfig = field(default_factory=ZhipuConfig)
    crawl: CrawlConfig = field(default_factory=CrawlConfig)
    flask: FlaskConfig = field(default_factory=FlaskConfig)
    display: DisplayConfig = field(default_factory=DisplayConfig)
    url: URLConfig = field(default_factory=URLConfig)
    xpath: XPathConfig = field(default_factory=XPathConfig)

    @classmethod
    def from_env(cls) -> "AppConfig":
        """从环境变量创建配置"""
        return cls()


config = AppConfig.from_env()
