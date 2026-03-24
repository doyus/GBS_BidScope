# -*- coding: utf-8 -*-
"""应用入口"""
from __future__ import annotations

from src.web.app import create_app

app = create_app()

if __name__ == "__main__":
    from src.config import settings

    print(f"数据库: {settings.database.db_path}")
    print(f"打开 http://{settings.web.host}:{settings.web.port}")
    app.run(
        host=settings.web.host,
        port=settings.web.port,
        debug=True,
    )
