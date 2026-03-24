# -*- coding: utf-8 -*-
"""测试配置"""
from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_db_path():
    """临时数据库路径"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    yield path
    os.unlink(path)


@pytest.fixture
def temp_db(temp_db_path):
    """临时数据库连接"""
    conn = sqlite3.connect(temp_db_path)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture
def sample_html():
    """示例HTML"""
    return """
    <html>
        <head><title>测试页面</title></head>
        <body>
            <article>
                <h1>测试标题</h1>
                <p>这是一段测试内容，包含中文文字和数字123。</p>
                <p>这是第二段内容，用于测试正文提取功能。</p>
            </article>
        </body>
    </html>
    """


@pytest.fixture
def sample_meta_dict():
    """示例元数据字典"""
    return {
        "标题": "测试标题",
        "主域名": "example.com",
        "来源": "测试来源",
        "aus_id": "12345",
    }


@pytest.fixture
def sample_meta_json():
    """示例元数据JSON"""
    return '{"标题": "测试标题", "主域名": "example.com", "来源": "测试来源"}'


@pytest.fixture
def mock_env_vars(monkeypatch):
    """模拟环境变量"""
    monkeypatch.setenv("ZHIPU_API_KEY", "test-api-key")
    monkeypatch.setenv("SQLITE_DB", ":memory:")
    monkeypatch.setenv("FLASK_SECRET_KEY", "test-secret-key")
