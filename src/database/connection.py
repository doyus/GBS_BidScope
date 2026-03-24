# -*- coding: utf-8 -*-
"""数据库连接管理模块"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Generator, Optional

from src.config import settings


class DatabaseManager:
    """数据库连接管理器"""

    _instance: Optional["DatabaseManager"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "DatabaseManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._db_path = settings.database.db_path
        self._local = threading.local()
        self._initialized = True

    def _get_connection(self) -> sqlite3.Connection:
        """获取线程本地连接"""
        if not hasattr(self._local, "connection") or self._local.connection is None:
            self._local.connection = sqlite3.connect(self._db_path)
            self._local.connection.row_factory = sqlite3.Row
            if settings.database.wal_mode:
                try:
                    self._local.connection.execute("PRAGMA journal_mode=WAL")
                except sqlite3.Error:
                    pass
        return self._local.connection

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """获取数据库连接的上下文管理器"""
        conn = self._get_connection()
        try:
            yield conn
        finally:
            pass  # 连接保持在线程本地

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        """事务上下文管理器"""
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def close(self) -> None:
        """关闭当前线程的连接"""
        if hasattr(self._local, "connection") and self._local.connection:
            self._local.connection.close()
            self._local.connection = None

    def close_all(self) -> None:
        """关闭所有连接（用于程序退出）"""
        self.close()


def get_db_manager() -> DatabaseManager:
    """获取数据库管理器实例"""
    return DatabaseManager()


@contextmanager
def get_connection() -> Generator[sqlite3.Connection, None, None]:
    """便捷函数：获取数据库连接"""
    manager = get_db_manager()
    with manager.get_connection() as conn:
        yield conn


@contextmanager
def get_transaction() -> Generator[sqlite3.Connection, None, None]:
    """便捷函数：获取事务连接"""
    manager = get_db_manager()
    with manager.transaction() as conn:
        yield conn
