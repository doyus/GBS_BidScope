# -*- coding: utf-8 -*-
"""URL处理工具模块"""
from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urlparse

import pandas as pd

from src.config import settings


def url_fingerprint(url: str | None) -> str:
    """生成URL指纹用于去重（统一host小写、去掉末尾无意义斜杠）"""
    if not url:
        return ""

    u = url.strip()
    if u.startswith("//"):
        u = "https:" + u

    parsed = urlparse(u)
    scheme = (parsed.scheme or "https").lower()
    netloc = (parsed.netloc or "").lower()
    path = (parsed.path or "").rstrip("/") or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    fragment = f"#{parsed.fragment}" if parsed.fragment else ""

    return f"{scheme}://{netloc}{path}{query}{fragment}"


def extract_domain(url: str) -> str:
    """从URL中提取域名"""
    host = urlparse(url).netloc.lower().replace("www.", "")
    return host or "unknown"


def get_domain_from_row(row: pd.Series, url: str) -> str:
    """从行数据或URL中获取域名"""
    if settings.URL_COL_HINTS[0] in row.index and pd.notna(
        row[settings.URL_COL_HINTS[0]]
    ):
        d = str(row[settings.URL_COL_HINTS[0]]).strip().lower()
        d = re.sub(r"^https?://", "", d).split("/")[0]
        if d:
            return d.replace("www.", "")
    return extract_domain(url)


def pick_url_from_row(row: pd.Series, url_columns: list[str]) -> Optional[str]:
    """从行中选取第一个有效的URL"""
    for col in url_columns:
        if col not in row.index or pd.isna(row[col]):
            continue

        u = str(row[col]).strip()
        if not u or u.lower() in ("nan", "none"):
            continue

        if u.startswith("http://") or u.startswith("https://"):
            return u
        if u.startswith("//"):
            return "https:" + u

    return None


def is_valid_url(text: str) -> bool:
    """检查文本是否是有效的URL"""
    return bool(re.match(r"^https?://", text.strip(), re.IGNORECASE))
