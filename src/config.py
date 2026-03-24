# -*- coding: utf-8 -*-
"""项目配置模块 - 集中管理所有配置项"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Final

# 尝试加载 .env 文件
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


@dataclass(frozen=True)
class DatabaseConfig:
    """数据库配置"""

    db_path: str
    wal_mode: bool = True


@dataclass(frozen=True)
class CrawlConfig:
    """爬虫配置"""

    min_text_len: int = 50
    min_cjk_article: int = 22
    min_valid_loose: int = 28
    max_retries: int = 0
    attempt_wait_sec: float = 1.0
    round_interval_sec: float = 600.0
    llm_html_max: int = 85000
    llm_json_max: int = 120000
    page_timeout: int = 5
    max_browser_restart: int = 15


@dataclass(frozen=True)
class ZhipuConfig:
    """智谱AI配置"""

    api_key: str
    model: str = "GLM-4-Flash-250414"


@dataclass(frozen=True)
class WebConfig:
    """Web服务器配置"""

    host: str = "127.0.0.1"
    port: int = 5050
    secret_key: str = "local-viewer-dev-key"
    json_as_ascii: bool = False


class Settings:
    """应用设置"""

    # URL列提示
    URL_COL_HINTS: Final[tuple[str, ...]] = (
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

    # 列表优先展示的Excel列
    LIST_META_PRIORITY: Final[tuple[str, ...]] = (
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

    # 清空时要删除的表
    TABLES_TO_CLEAR: Final[tuple[str, ...]] = (
        "cms_crawl_data_content",
        "crawl_url_dedup",
        "domain_learned_xpath",
        "domain_json_html_path",
    )

    # JSON请求头
    JSON_FETCH_HEADERS: Final[dict[str, str]] = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
    }

    # 浏览器异常触发词
    BROWSER_ERROR_TRIGGERS: Final[tuple[str, ...]] = (
        "disconnected",
        "connection refused",
        "target closed",
        "session",
        "chrome not reachable",
        "websocket",
        "devtools",
        "broken pipe",
        "no such window",
        "invalid session",
    )

    def __init__(self) -> None:
        base_dir = Path(__file__).parent.parent

        self.database = DatabaseConfig(
            db_path=os.environ.get("SQLITE_DB", str(base_dir / "crawl_local.db")),
            wal_mode=True,
        )

        self.crawl = CrawlConfig(
            min_text_len=int(os.environ.get("MIN_TEXT_LEN", "50")),
            min_cjk_article=int(os.environ.get("MIN_CJK_ARTICLE", "22")),
            min_valid_loose=int(os.environ.get("MIN_VALID_LOOSE", "28")),
            max_retries=int(os.environ.get("MAX_CRAWL_RETRIES", "0")),
            attempt_wait_sec=float(os.environ.get("ATTEMPT_WAIT_SEC", "1.0")),
            round_interval_sec=float(os.environ.get("ROUND_INTERVAL_SEC", "600.0")),
            llm_html_max=int(os.environ.get("LLM_HTML_MAX", "85000")),
            llm_json_max=int(os.environ.get("LLM_JSON_MAX", "120000")),
            page_timeout=int(os.environ.get("PAGE_TIMEOUT", "5")),
            max_browser_restart=int(os.environ.get("MAX_BROWSER_RESTART", "15")),
        )

        self.zhipu = ZhipuConfig(
            api_key=os.environ.get("ZHIPU_API_KEY", ""),
            model=os.environ.get("ZHIPU_MODEL", "GLM-4-Flash-250414"),
        )

        self.web = WebConfig(
            host=os.environ.get("FLASK_HOST", "127.0.0.1"),
            port=int(os.environ.get("PORT", "5050")),
            secret_key=os.environ.get("FLASK_SECRET_KEY", "local-viewer-dev-key"),
            json_as_ascii=False,
        )

        self.excel_path = os.environ.get("EXCEL_PATH", str(base_dir / "1.xlsx"))


# 全局设置实例
settings = Settings()
