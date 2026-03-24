# -*- coding: utf-8 -*-
"""
pytest配置和共享fixtures
"""
import os
import sqlite3
import tempfile
from typing import Generator

import pytest


@pytest.fixture
def temp_db() -> Generator[str, None, None]:
    """创建临时数据库文件"""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)

    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS cms_crawl_data_content (
            id INTEGER PRIMARY KEY,
            description TEXT,
            updated_at REAL,
            excel_meta TEXT,
            crawl_status TEXT,
            crawl_error TEXT,
            crawl_fail_count INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS crawl_url_dedup (
            url_key TEXT PRIMARY KEY,
            first_content_id INTEGER NOT NULL,
            created_at REAL
        );
        CREATE TABLE IF NOT EXISTS domain_learned_xpath (
            domain TEXT PRIMARY KEY,
            xpath TEXT NOT NULL,
            sample_url TEXT,
            updated_at REAL
        );
        CREATE TABLE IF NOT EXISTS domain_json_html_path (
            domain TEXT PRIMARY KEY,
            json_path TEXT NOT NULL,
            sample_url TEXT,
            updated_at REAL
        );
    """
    )
    conn.commit()
    conn.close()

    yield path

    try:
        os.unlink(path)
    except OSError:
        pass


@pytest.fixture
def sample_html() -> str:
    """示例HTML内容"""
    return """
    <html>
        <head><title>测试页面</title></head>
        <body>
            <div class="content">
                <h1>招标公告标题</h1>
                <p>这是一段测试内容，包含中文和数字123。</p>
                <p>另一段内容用于测试提取功能。</p>
            </div>
        </body>
    </html>
    """


@pytest.fixture
def sample_bidding_json() -> dict:
    """示例招投标JSON数据"""
    return {
        "data": {
            "tproject": {
                "projectName": "测试项目名称",
                "projectNo": "TEST-2026-001",
                "projectMessage": "项目说明内容",
                "projectAddress": "北京市朝阳区",
                "purchaseDept": "采购部门",
                "purchaserName": "张三",
                "mobile": "13800138000",
            },
            "processList": [
                {"processName": "发布公告", "createTime": "2026-03-01"},
                {"processName": "开标", "createTime": "2026-03-15"},
            ],
        }
    }
