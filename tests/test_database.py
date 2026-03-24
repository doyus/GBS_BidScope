# -*- coding: utf-8 -*-
"""数据库测试"""
from __future__ import annotations

import sqlite3

import pytest

from src.database.schema import (
    CREATE_TABLES_SQL,
    clear_tables,
    get_table_counts,
    init_database,
    migrate_table_schema,
)


class TestInitDatabase:
    """测试数据库初始化"""

    def test_creates_tables(self, temp_db_path):
        init_database(temp_db_path)

        conn = sqlite3.connect(temp_db_path)
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        conn.close()

        assert "cms_crawl_data_content" in tables
        assert "domain_learned_xpath" in tables
        assert "crawl_url_dedup" in tables
        assert "domain_json_html_path" in tables


class TestMigrateTableSchema:
    """测试表结构迁移"""

    def test_adds_missing_columns(self, temp_db):
        # 创建基础表
        temp_db.execute(
            """
            CREATE TABLE cms_crawl_data_content (
                id INTEGER PRIMARY KEY,
                description TEXT
            )
        """
        )
        temp_db.commit()

        # 迁移
        result = migrate_table_schema(temp_db)

        assert result is True

        # 验证列已添加
        cur = temp_db.execute("PRAGMA table_info(cms_crawl_data_content)")
        columns = {row[1] for row in cur.fetchall()}

        assert "excel_meta" in columns
        assert "crawl_status" in columns
        assert "crawl_error" in columns
        assert "crawl_fail_count" in columns

    def test_no_change_if_columns_exist(self, temp_db):
        # 创建完整表
        temp_db.executescript(CREATE_TABLES_SQL)
        temp_db.commit()

        # 迁移
        result = migrate_table_schema(temp_db)

        assert result is False


class TestGetTableCounts:
    """测试获取表记录数"""

    def test_returns_counts(self, temp_db):
        # 创建表并插入数据
        temp_db.executescript(CREATE_TABLES_SQL)
        temp_db.execute(
            "INSERT INTO cms_crawl_data_content (id, description) VALUES (1, 'test')"
        )
        temp_db.execute(
            "INSERT INTO cms_crawl_data_content (id, description) VALUES (2, 'test2')"
        )
        temp_db.commit()

        counts = get_table_counts(temp_db)

        assert counts["cms_crawl_data_content"] == 2

    def test_returns_negative_for_missing_table(self, temp_db):
        counts = get_table_counts(temp_db, ("nonexistent_table",))
        assert counts["nonexistent_table"] == -1


class TestClearTables:
    """测试清空表"""

    def test_clears_data_and_returns_before_counts(self, temp_db):
        # 创建表并插入数据
        temp_db.executescript(CREATE_TABLES_SQL)
        temp_db.execute(
            "INSERT INTO cms_crawl_data_content (id, description) VALUES (1, 'test')"
        )
        temp_db.commit()

        before = clear_tables(temp_db)

        assert before["cms_crawl_data_content"] == 1

        # 验证已清空
        cur = temp_db.execute("SELECT COUNT(*) FROM cms_crawl_data_content")
        assert cur.fetchone()[0] == 0
