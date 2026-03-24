# -*- coding: utf-8 -*-
import sqlite3

conn = sqlite3.connect("crawl_local.db")
cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [t[0] for t in cursor.fetchall()]
print("Tables:", tables)

for table in tables:
    print(f"\n=== Table: {table} ===")
    cursor = conn.execute(f"PRAGMA table_info({table})")
    columns = cursor.fetchall()
    for col in columns:
        print(f"  {col[1]} ({col[2]})")

    # 获取样本数据
    cursor = conn.execute(f"SELECT * FROM {table} LIMIT 2")
    rows = cursor.fetchall()
    if rows:
        print(f"  Sample rows: {len(rows)}")
        for row in rows[:1]:
            print(f"    {row}")

conn.close()
