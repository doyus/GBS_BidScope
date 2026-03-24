# -*- coding: utf-8 -*-
"""
database模块单元测试
"""
import sqlite3

import pytest

from database import (
    get_db_path,
    init_database,
    migrate_table_columns,
    get_connection,
    persist_crawl_result,
    persist_crawl_failure,
    get_url_dedup_id,
    save_url_dedup,
    get_domain_xpath,
    save_domain_xpath,
    get_domain_json_path,
    save_domain_json_path,
    clear_all_tables,
    get_table_counts,
)


class TestGetDbPath:
    """get_db_path函数测试"""

    def test_returns_string(self) -> None:
        """测试返回字符串"""
        result = get_db_path()
        assert isinstance(result, str)
        assert len(result) > 0


class TestInitDatabase:
    """init_database函数测试"""

    def test_creates_tables(self, temp_db: str) -> None:
        """测试创建表"""
        init_database(temp_db)

        conn = sqlite3.connect(temp_db)
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cur.fetchall()]
        conn.close()

        assert "cms_crawl_data_content" in tables
        assert "crawl_url_dedup" in tables


class TestMigrateTableColumns:
    """migrate_table_columns函数测试"""

    def test_adds_missing_columns(self, temp_db: str) -> None:
        """测试添加缺失列"""
        conn = sqlite3.connect(temp_db)
        conn.execute("DROP TABLE IF EXISTS cms_crawl_data_content")
        conn.execute(
            """
            CREATE TABLE cms_crawl_data_content (
                id INTEGER PRIMARY KEY,
                description TEXT
            )
        """
        )
        conn.commit()

        migrate_table_columns(conn)

        cur = conn.execute("PRAGMA table_info(cms_crawl_data_content)")
        columns = [row[1] for row in cur.fetchall()]
        conn.close()

        assert "excel_meta" in columns
        assert "crawl_status" in columns


class TestGetConnection:
    """get_connection函数测试"""

    def test_context_manager(self, temp_db: str) -> None:
        """测试上下文管理器"""
        init_database(temp_db)

        with get_connection(temp_db, check_exists=False) as conn:
            assert conn is not None

    def test_raises_on_missing_db(self) -> None:
        """测试数据库不存在时抛出异常"""
        with pytest.raises(FileNotFoundError):
            with get_connection("/nonexistent/path.db"):
                pass


class TestPersistCrawlResult:
    """persist_crawl_result函数测试"""

    def test_inserts_new_record(self, temp_db: str) -> None:
        """测试插入新记录"""
        init_database(temp_db)
        conn = sqlite3.connect(temp_db)

        persist_crawl_result(conn, 1, "test content", '{"key": "value"}')

        cur = conn.execute("SELECT * FROM cms_crawl_data_content WHERE id = 1")
        row = cur.fetchone()
        conn.close()

        assert row is not None
        assert row[1] == "test content"

    def test_updates_existing_record(self, temp_db: str) -> None:
        """测试更新现有记录"""
        init_database(temp_db)
        conn = sqlite3.connect(temp_db)

        persist_crawl_result(conn, 1, "first", "{}")
        persist_crawl_result(conn, 1, "second", "{}")

        cur = conn.execute(
            "SELECT description FROM cms_crawl_data_content WHERE id = 1"
        )
        row = cur.fetchone()
        conn.close()

        assert row[0] == "second"


class TestPersistCrawlFailure:
    """persist_crawl_failure函数测试"""

    def test_persists_failure(self, temp_db: str) -> None:
        """测试持久化失败记录"""
        init_database(temp_db)
        conn = sqlite3.connect(temp_db)

        persist_crawl_failure(conn, 1, "{}", "Connection timeout", 1, 3, False)

        cur = conn.execute(
            "SELECT crawl_status, crawl_error FROM cms_crawl_data_content WHERE id = 1"
        )
        row = cur.fetchone()
        conn.close()

        assert row[0] == "retrying"
        assert "timeout" in row[1].lower()


class TestUrlDedup:
    """URL去重功能测试"""

    def test_save_and_get(self, temp_db: str) -> None:
        """测试保存和获取"""
        init_database(temp_db)
        conn = sqlite3.connect(temp_db)

        save_url_dedup(conn, "https://example.com", 1)

        result = get_url_dedup_id(conn, "https://example.com")
        conn.close()

        assert result == 1

    def test_returns_none_for_missing(self, temp_db: str) -> None:
        """测试不存在的URL返回None"""
        init_database(temp_db)
        conn = sqlite3.connect(temp_db)

        result = get_url_dedup_id(conn, "https://nonexistent.com")
        conn.close()

        assert result is None


class TestDomainXPath:
    """域名XPath功能测试"""

    def test_save_and_get(self, temp_db: str) -> None:
        """测试保存和获取"""
        init_database(temp_db)
        conn = sqlite3.connect(temp_db)

        save_domain_xpath(conn, "example.com", "//div", "https://example.com")

        result = get_domain_xpath(conn, "example.com")
        conn.close()

        assert result == "//div"

    def test_returns_none_for_missing(self, temp_db: str) -> None:
        """测试不存在的域名返回None"""
        init_database(temp_db)
        conn = sqlite3.connect(temp_db)

        result = get_domain_xpath(conn, "nonexistent.com")
        conn.close()

        assert result is None


class TestDomainJsonPath:
    """域名JSON路径功能测试"""

    def test_save_and_get(self, temp_db: str) -> None:
        """测试保存和获取"""
        init_database(temp_db)
        conn = sqlite3.connect(temp_db)

        save_domain_json_path(
            conn, "example.com", "$.data.content", "https://example.com"
        )

        result = get_domain_json_path(conn, "example.com")
        conn.close()

        assert result == "$.data.content"


class TestClearAllTables:
    """clear_all_tables函数测试"""

    def test_clears_data(self, temp_db: str) -> None:
        """测试清空数据"""
        init_database(temp_db)
        conn = sqlite3.connect(temp_db)

        persist_crawl_result(conn, 1, "content", "{}")

        counts = clear_all_tables(conn)

        cur = conn.execute("SELECT COUNT(*) FROM cms_crawl_data_content")
        count = cur.fetchone()[0]
        conn.close()

        assert count == 0
        assert counts["cms_crawl_data_content"] == 1


class TestGetTableCounts:
    """get_table_counts函数测试"""

    def test_returns_counts(self, temp_db: str) -> None:
        """测试返回计数"""
        init_database(temp_db)
        conn = sqlite3.connect(temp_db)

        persist_crawl_result(conn, 1, "content", "{}")
        persist_crawl_result(conn, 2, "content", "{}")

        counts = get_table_counts(conn)
        conn.close()

        assert counts["cms_crawl_data_content"] == 2
