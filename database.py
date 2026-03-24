# -*- coding: utf-8 -*-
"""
数据库工具模块 - 统一数据库操作，消除重复代码
"""
from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from typing import Generator, Optional

from config import config


def get_db_path() -> str:
    """获取数据库路径"""
    return config.database.db_path


def init_database(db_path: Optional[str] = None) -> None:
    """初始化数据库表结构"""
    path = db_path or get_db_path()
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            f"""
            CREATE TABLE IF NOT EXISTS {config.database.table_content} (
                id INTEGER PRIMARY KEY,
                description TEXT,
                updated_at REAL,
                excel_meta TEXT,
                crawl_status TEXT,
                crawl_error TEXT,
                crawl_fail_count INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS {config.database.table_domain_xpath} (
                domain TEXT PRIMARY KEY,
                xpath TEXT NOT NULL,
                sample_url TEXT,
                updated_at REAL
            );
            CREATE TABLE IF NOT EXISTS {config.database.table_url_dedup} (
                url_key TEXT PRIMARY KEY,
                first_content_id INTEGER NOT NULL,
                created_at REAL
            );
            CREATE TABLE IF NOT EXISTS {config.database.table_domain_json_path} (
                domain TEXT PRIMARY KEY,
                json_path TEXT NOT NULL,
                sample_url TEXT,
                updated_at REAL
            );
            """
        )
        conn.commit()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
    finally:
        conn.close()


def migrate_table_columns(conn: sqlite3.Connection) -> None:
    """迁移表结构，添加缺失的列"""
    cur = conn.execute(f"PRAGMA table_info({config.database.table_content})")
    existing_columns = {row[1] for row in cur.fetchall()}

    columns_to_add = (
        ("excel_meta", "TEXT"),
        ("crawl_status", "TEXT"),
        ("crawl_error", "TEXT"),
        ("crawl_fail_count", "INTEGER DEFAULT 0"),
    )

    for col_name, col_type in columns_to_add:
        if col_name not in existing_columns:
            conn.execute(
                f"ALTER TABLE {config.database.table_content} "
                f"ADD COLUMN {col_name} {col_type}"
            )
            existing_columns.add(col_name)
    conn.commit()


@contextmanager
def get_connection(
    db_path: Optional[str] = None, check_exists: bool = True
) -> Generator[sqlite3.Connection, None, None]:
    """获取数据库连接的上下文管理器"""
    path = db_path or get_db_path()
    if check_exists:
        import os

        if not os.path.isfile(path):
            raise FileNotFoundError(f"数据库不存在: {path}")

    conn = sqlite3.connect(path)
    try:
        migrate_table_columns(conn)
        yield conn
    finally:
        conn.close()


def get_connection_simple(db_path: Optional[str] = None) -> sqlite3.Connection:
    """获取简单数据库连接（用于数据分析）"""
    path = db_path or get_db_path()
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def persist_crawl_result(
    conn: sqlite3.Connection,
    content_id: int,
    description: str,
    excel_meta_json: str,
    dry_run: bool = False,
) -> None:
    """持久化抓取成功结果"""
    if dry_run:
        return

    ts = time.time()
    exists = conn.execute(
        f"SELECT 1 FROM {config.database.table_content} WHERE id=?", (content_id,)
    ).fetchone()

    if exists:
        conn.execute(
            f"""UPDATE {config.database.table_content} 
               SET description=?, updated_at=?, excel_meta=?, 
                   crawl_status='ok', crawl_error='', crawl_fail_count=0
               WHERE id=?""",
            (description, ts, excel_meta_json, content_id),
        )
    else:
        conn.execute(
            f"""INSERT INTO {config.database.table_content}
               (id, description, updated_at, excel_meta, crawl_status, crawl_error, crawl_fail_count)
               VALUES (?,?,?,?,?,?,?)""",
            (content_id, description, ts, excel_meta_json, "ok", "", 0),
        )
    conn.commit()


def persist_crawl_failure(
    conn: sqlite3.Connection,
    content_id: int,
    excel_meta_json: str,
    error_summary: str,
    attempt_no: int,
    max_attempts: int,
    dry_run: bool = False,
) -> None:
    """持久化抓取失败记录"""
    if dry_run:
        return

    status = "failed" if attempt_no >= max_attempts else "retrying"
    error_msg = f"[{attempt_no}/{max_attempts}] {error_summary}"[:1900]
    ts = time.time()

    exists = conn.execute(
        f"SELECT 1 FROM {config.database.table_content} WHERE id=?", (content_id,)
    ).fetchone()

    if exists:
        conn.execute(
            f"""UPDATE {config.database.table_content}
               SET excel_meta=?, crawl_status=?, crawl_error=?, 
                   crawl_fail_count=?, updated_at=?
               WHERE id=?""",
            (excel_meta_json, status, error_msg, attempt_no, ts, content_id),
        )
    else:
        conn.execute(
            f"""INSERT INTO {config.database.table_content}
               (id, description, updated_at, excel_meta, crawl_status, crawl_error, crawl_fail_count)
               VALUES (?,?,?,?,?,?,?)""",
            (content_id, "", ts, excel_meta_json, status, error_msg, attempt_no),
        )
    conn.commit()


def get_url_dedup_id(conn: sqlite3.Connection, url_key: str) -> Optional[int]:
    """获取URL去重记录的内容ID"""
    row = conn.execute(
        f"SELECT first_content_id FROM {config.database.table_url_dedup} WHERE url_key=?",
        (url_key,),
    ).fetchone()
    return row[0] if row else None


def save_url_dedup(
    conn: sqlite3.Connection,
    url_key: str,
    content_id: int,
) -> None:
    """保存URL去重记录"""
    ts = time.time()
    conn.execute(
        f"""INSERT OR IGNORE INTO {config.database.table_url_dedup}
           (url_key, first_content_id, created_at) VALUES (?,?,?)""",
        (url_key, content_id, ts),
    )
    conn.commit()


def get_domain_xpath(conn: sqlite3.Connection, domain: str) -> Optional[str]:
    """获取域名已学习的XPath"""
    row = conn.execute(
        f"SELECT xpath FROM {config.database.table_domain_xpath} WHERE domain=?",
        (domain,),
    ).fetchone()
    return row[0] if row else None


def save_domain_xpath(
    conn: sqlite3.Connection,
    domain: str,
    xpath: str,
    sample_url: str,
) -> None:
    """保存域名XPath学习结果"""
    ts = time.time()
    conn.execute(
        f"""INSERT OR REPLACE INTO {config.database.table_domain_xpath}
           (domain, xpath, sample_url, updated_at) VALUES (?,?,?,?)""",
        (domain, xpath, sample_url, ts),
    )
    conn.commit()


def get_domain_json_path(conn: sqlite3.Connection, domain: str) -> Optional[str]:
    """获取域名JSON路径"""
    row = conn.execute(
        f"SELECT json_path FROM {config.database.table_domain_json_path} WHERE domain=?",
        (domain,),
    ).fetchone()
    return row[0] if row else None


def save_domain_json_path(
    conn: sqlite3.Connection,
    domain: str,
    json_path: str,
    sample_url: str,
) -> None:
    """保存域名JSON路径"""
    ts = time.time()
    conn.execute(
        f"""INSERT OR REPLACE INTO {config.database.table_domain_json_path}
           (domain, json_path, sample_url, updated_at) VALUES (?,?,?,?)""",
        (domain, json_path, sample_url, ts),
    )
    conn.commit()


def clear_all_tables(conn: sqlite3.Connection) -> dict[str, int]:
    """清空所有爬虫相关表，返回各表删除前的记录数"""
    tables = [
        config.database.table_content,
        config.database.table_url_dedup,
        config.database.table_domain_xpath,
        config.database.table_domain_json_path,
    ]

    counts: dict[str, int] = {}
    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            conn.execute(f"DELETE FROM {table}")
            counts[table] = count
        except sqlite3.OperationalError:
            counts[table] = -1
    conn.commit()
    return counts


def get_table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    """获取各表记录数"""
    tables = [
        config.database.table_content,
        config.database.table_url_dedup,
        config.database.table_domain_xpath,
        config.database.table_domain_json_path,
    ]

    counts: dict[str, int] = {}
    for table in tables:
        try:
            counts[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except sqlite3.OperationalError:
            counts[table] = -1
    return counts
