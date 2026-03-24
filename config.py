# -*- coding: utf-8 -*-
"""
配置文件：集中管理所有常量、硬编码值
"""
from __future__ import annotations

import os
from typing import Optional
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()

# =============================================================================
# 基础路径配置
# =============================================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH = os.environ.get("EXCEL_PATH", os.path.join(BASE_DIR, "1.xlsx"))
SQLITE_DB = os.environ.get("SQLITE_DB", os.path.join(BASE_DIR, "crawl_local.db"))
FLASK_SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "local-viewer-dev-key")
PORT = int(os.environ.get("PORT", "5050"))

# =============================================================================
# 智谱 AI 配置
# =============================================================================
ZHIPU_API_KEY = os.environ.get("ZHIPU_API_KEY", "")
ZHIPU_MODEL = os.environ.get("ZHIPU_MODEL", "GLM-4-Flash-250414")

# =============================================================================
# 数据库表名配置
# =============================================================================
DB_TABLES = {
    "content": "cms_crawl_data_content",
    "xpath": "domain_learned_xpath",
    "url_dedup": "crawl_url_dedup",
    "json_path": "domain_json_html_path",
}

# =============================================================================
# Excel 列名配置
# =============================================================================
COL_MAIN_DOMAIN = "主域名"
URL_COL_HINTS = (
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

# =============================================================================
# 抓取质量阈值
# =============================================================================
MIN_TEXT_LEN = 50  # 正文中中文+数字+字母个数最小值
MIN_CJK_ARTICLE = 22  # 至少多少汉字，避免 JSON/JS 凑数
MIN_VALID_LOOSE = 28  # 低于此值仍尝试用浏览器再抓
MAX_CRAWL_RETRIES_DEFAULT = 0  # 同一条任务内连续尝试的次数上限
ATTEMPT_WAIT_SEC_DEFAULT = 1  # 同一条任务两次尝试之间的等待（秒）
ROUND_INTERVAL_SEC_DEFAULT = 10  # 每跑完一整轮休眠多久再跑下一轮（分钟）

# =============================================================================
# LLM 输入限制
# =============================================================================
LLM_HTML_MAX = 85000  # 给大模型的 HTML 上限（字符）
LLM_JSON_MAX = 120000  # 给大模型的 JSON 文本上限

# =============================================================================
# HTTP 请求头配置
# =============================================================================
JSON_FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "Accept: application/json, text/plain, */*",
}

# =============================================================================
# 列表优先展示的 Excel 列
# =============================================================================
LIST_META_PRIORITY = (
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

# =============================================================================
# 静态 XPath 列表
# =============================================================================
STATIC_XPATHS: list[str] = [
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
]

# =============================================================================
# 需要清空的表（爬虫相关，不可恢复）
# =============================================================================
TABLES_TO_CLEAR = (
    "cms_crawl_data_content",
    "crawl_url_dedup",
    "domain_learned_xpath",
    "domain_json_html_path",
)

# =============================================================================
# 错误模式分类
# =============================================================================
ERROR_PATTERNS = {
    "timeout": r"timeout|timed out|连接超时",
    "network": r"network|connection|connect|网络|连接",
    "http_error": r"404|403|500|502|503|HTTP",
    "parse_error": r"parse|解析|extract",
    "content_error": r"content|empty|内容",
}


# =============================================================================
# 辅助函数
# =============================================================================
def get_db_path() -> str:
    """获取数据库路径"""
    return os.environ.get("SQLITE_DB", SQLITE_DB)
