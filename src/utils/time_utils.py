# -*- coding: utf-8 -*-
"""时间处理工具模块"""
from __future__ import annotations

from datetime import datetime
from typing import Optional


def parse_timestamp(ts: float | str | None) -> Optional[datetime]:
    """解析时间戳为datetime对象"""
    if ts is None:
        return None
    try:
        ts_val = float(ts)
        if ts_val > 1e12:
            ts_val = ts_val / 1000.0
        return datetime.fromtimestamp(ts_val)
    except (ValueError, TypeError, OSError):
        return None


def format_timestamp(ts: float | str | None) -> str:
    """格式化时间戳为可读字符串"""
    dt = parse_timestamp(ts)
    if dt is None:
        return "—"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_date(ts: float | str | None) -> Optional[str]:
    """格式化时间戳为日期字符串"""
    dt = parse_timestamp(ts)
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%d")


def get_date_range(
    timestamps: list[float | str | None],
) -> dict[str, Optional[str]]:
    """获取日期范围"""
    dates = [d for d in [format_date(ts) for ts in timestamps] if d is not None]
    if not dates:
        return {"start": None, "end": None}
    return {"start": min(dates), "end": max(dates)}
