# -*- coding: utf-8 -*-
"""数据库表结构管理模块"""
from __future__ import annotations

import sqlite3
from typing import Callable

from src.config import settings

# DDL语句
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS cms_crawl_data_content (
    id INTEGER PRIMARY KEY,
    description TEXT,
    updated_at REAL,
    excel_meta TEXT,
    crawl_status TEXT,
    crawl_error TEXT,
    crawl_fail_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS domain_learned_xpath (
    domain TEXT PRIMARY KEY,
    xpath TEXT NOT NULL,
    sample_url TEXT,
    updated_at REAL
);

CREATE TABLE IF NOT EXISTS crawl_url_dedup (
    url_key TEXT PRIMARY KEY,
    first_content_id INTEGER NOT NULL,
    created_at REAL
);

CREATE TABLE IF NOT EXISTS domain_json_html_path (
    domain TEXT PRIMARY KEY,
    json_path TEXT NOT NULL,
    sample_url TEXT,
    updated_at REAL
);
"""

# 需要添加的列（迁移用）
MIGRATION_COLUMNS = [
    ("excel_meta", "TEXT"),
    ("crawl_status", "TEXT"),
    ("crawl_error", "TEXT"),
    ("crawl_fail_count", "INTEGER DEFAULT 0"),
]


def init_database(db_path: str | None = None) -> None:
    """初始化数据库表结构"""
    path = db_path or settings.database.db_path
    conn = sqlite3.connect(path)
    try:
        conn.executescript(CREATE_TABLES_SQL)
        conn.commit()

        # 启用WAL模式
        if settings.database.wal_mode:
            try:
                conn.execute("PRAGMA journal_mode=WAL")
            except sqlite3.Error:
                pass
    finally:
        conn.close()


def migrate_table_schema(
    conn: sqlite3.Connection,
    table_name: str = "cms_crawl_data_content",
    columns: list[tuple[str, str]] | None = None,
) -> bool:
    """迁移表结构，添加缺失的列"""
    cols = columns or MIGRATION_COLUMNS

    cur = conn.execute(f"PRAGMA table_info({table_name})")
    existing_cols = {row[1] for row in cur.fetchall()}

    added = False
    for col_name, col_type in cols:
        if col_name not in existing_cols:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
            existing_cols.add(col_name)
            added = True

    if added:
        conn.commit()

    return added


def get_table_counts(
    conn: sqlite3.Connection, tables: tuple[str, ...] | None = None
) -> dict[str, int]:
    """获取各表的记录数"""
    table_list = tables or settings.TABLES_TO_CLEAR
    counts: dict[str, int] = {}

    for table in table_list:
        try:
            result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            counts[table] = result[0] if result else 0
        except sqlite3.OperationalError:
            counts[table] = -1

    return counts


def clear_tables(
    conn: sqlite3.Connection, tables: tuple[str, ...] | None = None
) -> dict[str, int]:
    """清空指定表，返回删除前的记录数"""
    table_list = tables or settings.TABLES_TO_CLEAR
    before_counts: dict[str, int] = {}

    for table in table_list:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            conn.execute(f"DELETE FROM {table}")
            before_counts[table] = count
        except sqlite3.OperationalError:
            before_counts[table] = -1

    conn.commit()
    return before_counts


def with_migrated_connection(
    func: Callable[[sqlite3.Connection], None],
    db_path: str | None = None,
) -> None:
    """在迁移后的连接上执行函数"""
    path = db_path or settings.database.db_path
    conn = sqlite3.connect(path)
    try:
        migrate_table_schema(conn)
        func(conn)
    finally:
        conn.close()
