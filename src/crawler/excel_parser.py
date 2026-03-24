# -*- coding: utf-8 -*-
"""Excel解析模块"""
from __future__ import annotations

import json
import re
from typing import Any, Optional

import pandas as pd

from src.config import settings


def clean_column_name(name: Any) -> str:
    """清理列名"""
    s = str(name).strip()
    s = s.replace("\xa0", "").replace("\u3000", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """规范化DataFrame列名"""
    df = df.copy()
    df.columns = [clean_column_name(c) for c in df.columns]
    df.columns = [c if c else f"_col_{i}" for i, c in enumerate(df.columns)]
    return df


def series_has_urls(series: pd.Series, min_hits: int = 2, sample: int = 80) -> bool:
    """检查Series是否包含URL"""
    s = series.dropna().astype(str).str.strip().head(sample)
    if len(s) == 0:
        return False

    hits = s.str.match(r"https?://", case=False, na=False).sum()
    return hits >= min_hits or (hits >= 1 and hits >= max(1, len(s) // 10))


def detect_url_columns(
    df: pd.DataFrame, force_names: Optional[list[str]] = None
) -> list[str]:
    """检测URL列"""
    if force_names:
        return _get_forced_columns(df, force_names)

    return _score_and_select_columns(df)


def _get_forced_columns(df: pd.DataFrame, force_names: list[str]) -> list[str]:
    """获取强制指定的列"""
    out = []
    for name in force_names:
        name = name.strip()
        if not name:
            continue

        if name in df.columns:
            out.append(name)
            continue

        # 尝试大小写不敏感匹配
        for col in df.columns:
            if str(col).strip().lower() == name.lower():
                out.append(col)
                break

    return out


def _score_and_select_columns(df: pd.DataFrame) -> list[str]:
    """评分并选择URL列"""
    scored: list[tuple[int, str]] = []

    for col in df.columns:
        cstr = str(col)
        cl = cstr.lower()
        score = 0

        # URL匹配得分
        if series_has_urls(df[col]):
            score += 50

        # 列名提示得分
        for hint in settings.URL_COL_HINTS:
            if hint.lower() == cl or hint == cstr:
                score += 30
                break
            if hint.lower() in cl or hint in cstr:
                score += 15

        # 关键词得分
        if any(k in cstr for k in ("详情", "正文", "公告")):
            score += 8
        if (
            "content" in cl
            or "链接" in cstr
            or "url" in cl
            or "link" in cl
            or "地址" in cstr
        ):
            score += 5

        if score > 0:
            scored.append((score, col))

    scored.sort(key=lambda x: -x[0])
    cols = [c for _, c in scored]

    # 如果没有找到，尝试宽松匹配
    if not cols:
        for col in df.columns:
            if series_has_urls(df[col], min_hits=1, sample=200):
                cols.append(col)

    return cols


def parse_id_cell(value: Any) -> Optional[int]:
    """解析ID单元格"""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return None

    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def resolve_content_id(row: pd.Series) -> Optional[int]:
    """解析内容ID（优先id列，否则aus_id）"""
    id_col = aus_col = None

    for c in row.index:
        cl = str(c).strip().lower()
        if cl == "id":
            id_col = c
        elif cl in ("aus_id", "ausid") or (
            "aus" in cl.replace(" ", "") and "id" in cl and cl != "id"
        ):
            if aus_col is None:
                aus_col = c

    if id_col is not None:
        v = parse_id_cell(row[id_col])
        if v is not None:
            return v

    if aus_col is not None:
        v = parse_id_cell(row[aus_col])
        if v is not None:
            return v

    return None


def row_to_json_meta(row: pd.Series) -> str:
    """将行数据转换为JSON元数据"""
    data: dict[str, Any] = {}

    for k, v in row.items():
        kn = str(k).strip()
        if not kn:
            continue

        if pd.isna(v):
            data[kn] = None
        elif isinstance(v, pd.Timestamp):
            data[kn] = v.isoformat()
        elif isinstance(v, (str, int, float, bool)):
            data[kn] = v
        else:
            try:
                x = v.item()
                if isinstance(x, (str, int, float, bool)) or x is None:
                    data[kn] = x
                else:
                    data[kn] = str(v)
            except Exception:
                data[kn] = str(v)

    return json.dumps(data, ensure_ascii=False)


def pick_url_from_row(row: pd.Series, url_columns: list[str]) -> Optional[str]:
    """从行中选取URL"""
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
