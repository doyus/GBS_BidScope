# -*- coding: utf-8 -*-
"""
通用工具模块 - 提取公共函数，遵循DRY原则
"""
from __future__ import annotations

import html as html_module
import json
import re
from datetime import datetime
from typing import Any, Optional
from urllib.parse import urlparse

import pandas as pd



def parse_meta(raw: Optional[str]) -> dict[str, Any]:
    """解析Excel元数据JSON"""
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def strip_tags(text: Optional[str]) -> str:
    """去除HTML标签"""
    if not text:
        return ""
    result = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", result).strip()


def strip_tags_preview(text: Optional[str], max_len: int = 100) -> str:
    """去除HTML标签并截取预览"""
    if not text:
        return ""
    clean = strip_tags(text)
    if len(clean) <= max_len:
        return clean
    return clean[:max_len] + "…"


def count_valid_text(html_or_text: Optional[str]) -> int:
    """统计有效文本字符数（中文+数字+字母）"""
    if not html_or_text:
        return 0
    plain = strip_tags(html_or_text)
    cjk_count = len(re.findall(r"[\u4e00-\u9fff]", plain))
    digit_count = len(re.findall(r"\d", plain))
    alpha_count = len(re.findall(r"[A-Za-z]", plain))
    return cjk_count + digit_count + alpha_count


def count_text_stats(html: Optional[str]) -> dict[str, int]:
    """统计文本详细数据"""
    if not html:
        return {"cn": 0, "digit": 0, "alpha": 0, "total": 0}

    plain = strip_tags(html)
    cn = len(re.findall(r"[\u4e00-\u9fff]", plain))
    digit = len(re.findall(r"\d", plain))
    alpha = len(re.findall(r"[A-Za-z]", plain))

    return {
        "cn": cn,
        "digit": digit,
        "alpha": alpha,
        "total": cn + digit + alpha,
    }


def format_timestamp(ts: Optional[Any]) -> str:
    """格式化时间戳为可读字符串"""
    if ts is None:
        return "—"
    try:
        ts_val = float(ts)
        if ts_val > 1e12:
            ts_val = ts_val / 1000.0
        return datetime.fromtimestamp(ts_val).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return str(ts)


def parse_timestamp(ts: Optional[Any]) -> Optional[datetime]:
    """解析时间戳为datetime对象"""
    if ts is None:
        return None
    try:
        ts_val = float(ts)
        if ts_val > 1e12:
            ts_val = ts_val / 1000.0
        return datetime.fromtimestamp(ts_val)
    except (TypeError, ValueError):
        return None


def shorten_url(url: Optional[str], max_len: int = 42) -> str:
    """缩短URL显示"""
    if not url:
        return "—"
    url = str(url).strip()
    if len(url) <= max_len:
        return url
    return url[:max_len] + "…"


def url_fingerprint(url: Optional[str]) -> str:
    """生成URL指纹用于去重"""
    if not url:
        return ""

    url = url.strip()
    if not url:
        return ""

    if url.startswith("//"):
        url = "https:" + url

    parsed = urlparse(url)
    scheme = (parsed.scheme or "https").lower()
    netloc = (parsed.netloc or "").lower()
    path = (parsed.path or "").rstrip("/") or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    fragment = f"#{parsed.fragment}" if parsed.fragment else ""

    return f"{scheme}://{netloc}{path}{query}{fragment}"


def extract_domain(url: str) -> str:
    """从URL提取域名"""
    if not url:
        return "unknown"
    parsed = urlparse(url)
    domain = parsed.netloc.lower().replace("www.", "")
    return domain or "unknown"


def pandas_row_to_json(row: pd.Series) -> str:
    """将pandas行转换为JSON字符串"""
    result: dict[str, Any] = {}

    for key, value in row.items():
        key_name = str(key).strip()
        if not key_name:
            continue

        if pd.isna(value):
            result[key_name] = None
        elif isinstance(value, pd.Timestamp):
            result[key_name] = value.isoformat()
        elif isinstance(value, (str, int, float, bool)):
            result[key_name] = value
        else:
            try:
                scalar = value.item()
                if isinstance(scalar, (str, int, float, bool)) or scalar is None:
                    result[key_name] = scalar
                else:
                    result[key_name] = str(value)
            except (AttributeError, ValueError):
                result[key_name] = str(value)

    return json.dumps(result, ensure_ascii=False)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """规范化DataFrame列名"""
    df = df.copy()

    def clean_header(header: Any) -> str:
        s = str(header).strip()
        s = s.replace("\xa0", "").replace("\u3000", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    df.columns = [clean_header(c) for c in df.columns]
    df.columns = [c if c else f"_col_{i}" for i, c in enumerate(df.columns)]

    return df


def parse_id_value(value: Any) -> Optional[int]:
    """解析ID值"""
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None

    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return None

    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def escape_html(text: Optional[Any]) -> str:
    """HTML转义"""
    if text is None:
        return ""
    s = "" if text is None else str(text).strip()
    if not s or s.lower() in ("null", "none"):
        return ""
    return html_module.escape(s).replace("\n", "<br/>\n")


def build_status_where(status: str) -> tuple[str, tuple]:
    """构建状态筛选SQL条件"""
    status = (status or "all").strip().lower()

    if status == "ok":
        return (
            " (IFNULL(crawl_status,'') IN ('ok','') "
            "AND (IFNULL(crawl_error,'')='' OR crawl_status='ok') "
            "AND (LENGTH(IFNULL(description,''))>80 OR crawl_status='ok')) ",
            (),
        )
    if status == "failed":
        return (" IFNULL(crawl_status,'') = 'failed' ", ())
    if status == "retrying":
        return (" IFNULL(crawl_status,'') = 'retrying' ", ())
    if status == "problem":
        return (
            " (IFNULL(crawl_status,'') IN ('failed','retrying') "
            "OR (IFNULL(crawl_error,'')!='' AND IFNULL(crawl_status,'')!='ok')) ",
            (),
        )
    return (" 1=1 ", ())


def get_order_by_sql() -> str:
    """获取排序SQL"""
    return (
        "(CASE WHEN updated_at IS NULL OR TRIM(CAST(updated_at AS TEXT)) "
        "IN ('','0') THEN 0.0 "
        "WHEN ABS(CAST(updated_at AS REAL)) > 1e11 "
        "THEN CAST(updated_at AS REAL) / 1000.0 "
        "ELSE CAST(updated_at AS REAL) END) DESC, id DESC"
    )
