# -*- coding: utf-8 -*-
"""
爬虫入口

运行: python crawl.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent))

from src.config import settings
from src.crawler.excel_parser import (
    detect_url_columns,
    normalize_columns,
    resolve_content_id,
)
from src.database.schema import init_database


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="招投标数据爬虫")
    parser.add_argument(
        "--excel", "-e",
        default=settings.excel_path,
        help="Excel文件路径",
    )
    parser.add_argument(
        "--db", "-d",
        default=settings.database.db_path,
        help="数据库路径",
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="初始化数据库",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="试运行模式（不写入数据库）",
    )

    args = parser.parse_args()

    # 初始化数据库
    if args.init_db:
        init_database(args.db)
        print(f"数据库已初始化: {args.db}")
        return

    # 这里可以添加爬虫主逻辑
    print(f"Excel文件: {args.excel}")
    print(f"数据库: {args.db}")
    print("爬虫功能待实现...")


if __name__ == "__main__":
    main()
