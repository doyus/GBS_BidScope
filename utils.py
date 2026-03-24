# -*- coding: utf-8 -*-
"""
工具模块：公共函数集合
"""
from __future__ import annotations

import html as html_module
import json
import re
import sqlite3
import threading
import time
from contextlib import nullcontext
from datetime import datetime, timedelta
from typing import Any, Callable, Optional, Union
from urllib.parse import urlparse

import pandas as pd
from lxml import etree, html as lxml_html

from config import (
    ERROR_PATTERNS,
    LLM_HTML_MAX,
    LLM_JSON_MAX,
    MIN_CJK_ARTICLE,
    MIN_TEXT_LEN,
    MIN_VALID_LOOSE,
    URL_COL_HINTS,
    get_db_path,
)


# =============================================================================
# 数据库相关工具函数
# =============================================================================
def get_db_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    """获取数据库连接"""
    path = db_path or get_db_path()
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_sqlite(db_path: str) -> None:
    """初始化 SQLite 数据库表结构"""
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
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
        )
        conn.commit()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
    finally:
        conn.close()


def migrate_db_schema(conn: sqlite3.Connection) -> None:
    """迁移数据库表结构"""
    cur = conn.execute("PRAGMA table_info(cms_crawl_data_content)")
    have = {r[1] for r in cur.fetchall()}
    added = False

    schema_columns = [
        ("excel_meta", "TEXT"),
        ("crawl_status", "TEXT"),
        ("crawl_error", "TEXT"),
        ("crawl_fail_count", "INTEGER DEFAULT 0"),
    ]

    for col, ddl in schema_columns:
        if col not in have:
            conn.execute(f"ALTER TABLE cms_crawl_data_content ADD COLUMN {col} {ddl}")
            added = True
            have.add(col)

    if added:
        conn.commit()


# =============================================================================
# 元数据解析工具函数
# =============================================================================
def parse_meta(raw: Optional[str]) -> dict[str, Any]:
    """解析 Excel 元数据 JSON"""
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def pandas_row_to_excel_meta(row: pd.Series) -> str:
    """Excel 一行 → JSON 字符串（列名→值）"""
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
                item = value.item()  # numpy scalar
                if isinstance(item, (str, int, float, bool)) or item is None:
                    result[key_name] = item
                else:
                    result[key_name] = str(value)
            except Exception:
                result[key_name] = str(value)
    return json.dumps(result, ensure_ascii=False)


# =============================================================================
# 文本处理工具函数
# =============================================================================
def strip_tags(text: str) -> str:
    """去除 HTML 标签，返回纯文本"""
    if not text:
        return ""
    cleaned = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", cleaned).strip()


def strip_tags_preview(text: str, max_len: int = 100) -> str:
    """去除 HTML 标签，返回预览文本（带省略号）"""
    if not text:
        return ""
    cleaned = strip_tags(text)
    return cleaned[:max_len] + ("…" if len(cleaned) > max_len else "")


def valid_text_cjk_digit_alpha(html_or_text: str) -> int:
    """去标签后统计：中文(CJK) + 数字 + 字母（不计标点空格）"""
    plain = re.sub(r"<[^>]+>", " ", html_or_text or "")
    plain = re.sub(r"\s+", " ", plain)
    return (
        len(re.findall(r"[\u4e00-\u9fff]", plain))
        + len(re.findall(r"\d", plain))
        + len(re.findall(r"[A-Za-z]", plain))
    )


def body_text_stats(html: str) -> dict[str, int]:
    """去标签后统计：中文(CJK)、数字、字母；不计空格标点"""
    plain = re.sub(r"<[^>]+>", " ", html or "")
    plain = re.sub(r"\s+", " ", plain)
    cn = len(re.findall(r"[\u4e00-\u9fff]", plain))
    digit = len(re.findall(r"\d", plain))
    alpha = len(re.findall(r"[A-Za-z]", plain))
    return {
        "cn": cn,
        "digit": digit,
        "alpha": alpha,
        "total": cn + digit + alpha,
    }


def short_url(url: str, max_length: int = 42) -> str:
    """缩短 URL 显示"""
    if not url:
        return "—"
    url_str = str(url).strip()
    return url_str[:max_length] + ("…" if len(url_str) > max_length else "")


def calc_content_length(html: Optional[str]) -> int:
    """计算 HTML 内容长度（有效字符）"""
    if not html:
        return 0
    plain = re.sub(r"<[^>]+>", " ", html)
    plain = re.sub(r"\s+", " ", plain)
    return (
        len(re.findall(r"[\u4e00-\u9fff]", plain))
        + len(re.findall(r"\d", plain))
        + len(re.findall(r"[A-Za-z]", plain))
    )


# =============================================================================
# URL 和域名处理工具函数
# =============================================================================
def url_fingerprint(url: str) -> str:
    """同一公告 URL 去重用（统一 host 小写、去掉末尾无意义斜杠）"""
    url_str = (url or "").strip()
    if not url_str:
        return ""
    if url_str.startswith("//"):
        url_str = "https:" + url_str

    parsed = urlparse(url_str)
    scheme = (parsed.scheme or "https").lower()
    netloc = (parsed.netloc or "").lower()
    path = (parsed.path or "").rstrip("/") or "/"
    query = f"?{parsed.query}" if parsed.query else ""
    fragment = f"#{parsed.fragment}" if parsed.fragment else ""

    return f"{scheme}://{netloc}{path}{query}{fragment}"


def domain_key_for_row(row: pd.Series, url: str) -> str:
    """从行数据或 URL 提取域名字段"""
    from config import COL_MAIN_DOMAIN

    if COL_MAIN_DOMAIN in row.index and pd.notna(row[COL_MAIN_DOMAIN]):
        domain = str(row[COL_MAIN_DOMAIN]).strip().lower()
        domain = re.sub(r"^https?://", "", domain).split("/")[0]
        if domain:
            return domain.replace("www.", "")

    host = urlparse(url).netloc.lower().replace("www.", "")
    return host or "unknown"


def pick_url(row: pd.Series, url_columns: list[str]) -> Optional[str]:
    """从行数据中选取有效的 URL"""
    for col in url_columns:
        if col not in row.index or pd.isna(row[col]):
            continue
        url = str(row[col]).strip()
        if not url or url.lower() in ("nan", "none"):
            continue
        if url.startswith("http://") or url.startswith("https://"):
            return url
        if url.startswith("//"):
            return "https:" + url
    return None


# =============================================================================
# Excel 处理工具函数
# =============================================================================
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """规范化 DataFrame 列名"""
    df = df.copy()

    def _clean_header(x: Any) -> str:
        s = str(x).strip()
        s = s.replace("\xa0", "").replace("\u3000", " ")
        s = re.sub(r"\s+", "", s).strip()  # 移除所有空格
        return s

    df.columns = [_clean_header(c) for c in df.columns]
    df.columns = [c if c else f"_col_{i}" for i, c in enumerate(df.columns)]
    return df


def _series_has_urls(series: pd.Series, min_hits: int = 2, sample: int = 80) -> bool:
    """检查 Series 是否包含 URL"""
    series = series.dropna().astype(str).str.strip().head(sample)
    if len(series) == 0:
        return False
    hits = series.str.match(r"https?://", case=False, na=False).sum()
    result = hits >= min_hits or (hits >= 1 and hits >= max(1, len(series) // 10))
    return bool(result)


def detect_url_columns(
    df: pd.DataFrame, force_names: Optional[list[str]] = None
) -> list[str]:
    """按优先级返回「详情链接」列名列表"""
    if force_names:
        result = []
        for name in force_names:
            name = name.strip()
            if not name:
                continue
            if name in df.columns:
                result.append(name)
                continue
            for col in df.columns:
                if str(col).strip() == name or str(col).strip().lower() == name.lower():
                    result.append(col)
                    break
        if result:
            return result
        return []

    scored: list[tuple[int, str]] = []
    for col in df.columns:
        col_str = str(col)
        col_lower = col_str.lower()
        score = 0
        if _series_has_urls(df[col]):
            score += 50
        for hint in URL_COL_HINTS:
            if hint.lower() == col_lower or hint == col_str:
                score += 30
                break
            if hint.lower() in col_lower or hint in col_str:
                score += 15
        if any(k in col_str for k in ("详情", "正文", "公告")):
            score += 8
        if (
            "content" in col_lower
            or "链接" in col_str
            or "url" in col_lower
            or "link" in col_lower
            or "地址" in col_str
        ):
            score += 5
        if score > 0:
            scored.append((score, col))

    scored.sort(key=lambda x: -x[0])
    cols = [c for _, c in scored]

    if cols:
        return cols

    for col in df.columns:
        if _series_has_urls(df[col], min_hits=1, sample=200):
            cols.append(col)
    return cols


# =============================================================================
# ID 解析工具函数
# =============================================================================
def _parse_id_cell(value: Any) -> Optional[int]:
    """解析 ID 单元格值"""
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
    """优先列名 id，否则 aus_id（大小写/空格变体）"""
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
        v = _parse_id_cell(row[id_col])
        if v is not None:
            return v
    if aus_col is not None:
        v = _parse_id_cell(row[aus_col])
        if v is not None:
            return v
    return None


# =============================================================================
# 时间处理工具函数
# =============================================================================
def format_timestamp(ts: Any) -> str:
    """格式化时间戳"""
    if ts is None:
        return "—"
    try:
        ts_float = float(ts)
        if ts_float > 1e12:
            ts_float = ts_float / 1000.0
        return datetime.fromtimestamp(ts_float).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


def parse_timestamp(ts: Any) -> Optional[datetime]:
    """解析时间戳"""
    if ts is None:
        return None
    try:
        ts_val = float(ts)
        if ts_val > 1e12:
            ts_val = ts_val / 1000.0
        return datetime.fromtimestamp(ts_val)
    except Exception:
        return None


# =============================================================================
# HTML 提取工具函数
# =============================================================================
def extract_by_xpaths(html: str, xpaths: list[str]) -> str:
    """通过 XPath 列表提取内容"""
    try:
        tree = lxml_html.fromstring(html)
    except Exception:
        return ""
    for xp in xpaths:
        try:
            nodes = tree.xpath(xp)
            for n in nodes:
                if not hasattr(n, "tag"):
                    continue
                frag = etree.tostring(n, encoding="unicode", method="html")
                if is_good_main_content(frag):
                    return frag
        except Exception:
            continue
    return ""


def apply_single_xpath(html: str, xpath: str) -> str:
    """应用单个 XPath"""
    xpath = (xpath or "").strip()
    if not xpath:
        return ""
    try:
        tree = lxml_html.fromstring(html)
        nodes = tree.xpath(xpath)
        if not nodes:
            return ""
        n = nodes[0]
        if not hasattr(n, "tag"):
            return str(n) if n else ""
        return etree.tostring(n, encoding="unicode", method="html")
    except Exception:
        return ""


def is_good_main_content(html_fragment: str) -> bool:
    """检查是否是有效的主要内容"""
    if not html_fragment or len(html_fragment) < 80:
        return False
    valid_chars = valid_text_cjk_digit_alpha(html_fragment)
    if valid_chars < MIN_TEXT_LEN:
        return False
    link_ratio = html_fragment.lower().count("<a ") / max(valid_chars / 80, 1)
    if link_ratio > 8:
        return False
    return True


def extract_largest_text_div(html: str) -> str:
    """选择 body 下文本量最大、链接占比相对低的 div（启发式）"""
    try:
        tree = lxml_html.fromstring(html)
    except Exception:
        return ""

    body = tree.find(".//body")
    if body is None:
        return ""

    best_html = ""
    best_score = 0.0

    for div in body.iter("div"):
        try:
            text = "".join(div.itertext() or [])
            if valid_text_cjk_digit_alpha(text) < MIN_VALID_LOOSE:
                continue
            a_count = len(div.xpath(".//a"))
            depth = len(list(div.iterancestors()))
            score = len(text) - 15 * min(a_count, 50) - 2 * max(0, depth - 15)
            if score > best_score:
                best_score = score
                best_html = etree.tostring(div, encoding="unicode", method="html")
        except Exception:
            continue

    return best_html if is_good_main_content(best_html) else ""


# =============================================================================
# JSON 处理工具函数
# =============================================================================
def _try_parse_json_loose(text: str) -> Optional[Any]:
    """从文本中尽量解析出 JSON 对象"""
    s = (text or "").strip()
    if not s:
        return None
    s = html_module.unescape(s)
    if s[0] not in "{[":
        return None
    try:
        return json.loads(s)
    except Exception:
        pass
    i = s.find("{")
    j = s.rfind("}")
    if i >= 0 and j > i:
        try:
            return json.loads(s[i : j + 1])
        except Exception:
            pass
    return None


def json_path_get(root: Any, path: str) -> Any:
    """从根对象按路径取值"""
    path = (path or "").strip().strip("$").strip(".")
    if not path:
        return root

    current: Any = root
    i, n = 0, len(path)

    while i < n:
        while i < n and path[i] in ". ":
            i += 1
        if i >= n:
            break
        if path[i] == "[":
            j = path.find("]", i)
            if j < 0:
                return None
            try:
                idx = int(path[i + 1 : j].strip())
            except ValueError:
                return None
            if not isinstance(current, list) or idx < 0 or idx >= len(current):
                return None
            current = current[idx]
            i = j + 1
        else:
            j = i
            while j < n and path[j] not in ".[":
                j += 1
            key = path[i:j].strip().strip('"').strip("'")
            if not isinstance(current, dict):
                return None
            if key not in current:
                for k in current:
                    if str(k).strip() == key:
                        current = current[k]
                        break
                else:
                    return None
            else:
                current = current[key]
            i = j

    return current


def stringify_json_sample(data: Any) -> str:
    """将 JSON 数据转换为字符串样本"""
    try:
        s = json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        s = str(data)
    return s[:LLM_JSON_MAX]


def guess_longest_html_in_json(obj: Any, depth: int = 0, max_depth: int = 18) -> str:
    """递归找最长的、像 HTML 的字符串"""
    best = ""
    if depth > max_depth or obj is None:
        return best
    if isinstance(obj, str):
        if (
            len(obj) > len(best)
            and "<" in obj
            and ("</" in obj or "<div" in obj.lower() or "<p" in obj.lower())
        ):
            return obj
        return ""
    if isinstance(obj, dict):
        for v in obj.values():
            sub = guess_longest_html_in_json(v, depth + 1, max_depth)
            if len(sub) > len(best):
                best = sub
    elif isinstance(obj, list):
        for v in obj[:100]:
            sub = guess_longest_html_in_json(v, depth + 1, max_depth)
            if len(sub) > len(best):
                best = sub
    return best


def try_common_json_html_keys(obj: Any) -> str:
    """常见 API 字段名试探"""
    keys = (
        "content",
        "html",
        "htmlContent",
        "contentHtml",
        "noticeContent",
        "detailContent",
        "articleContent",
        "ggnr",
        "zbggnr",
        "noticeHtml",
        "mainContent",
        "body",
    )
    nest = ("data", "result", "detail", "notice", "obj", "datas", "rows", "record")

    def walk(x: Any) -> str:
        if isinstance(x, dict):
            for k in keys:
                v = x.get(k)
                if isinstance(v, str) and "<" in v and len(v) > 80:
                    return v
            for k in nest:
                if k in x and isinstance(x[k], (dict, list)):
                    h = walk(x[k])
                    if h:
                        return h
        elif isinstance(x, list) and x:
            return walk(x[0])
        return ""

    return walk(obj)


# =============================================================================
# 数据库持久化工具函数
# =============================================================================
def persist_crawl_failure(
    conn: sqlite3.Connection,
    content_id: int,
    excel_meta_json: str,
    err_summary: str,
    attempt_no: int,
    max_attempts: int,
    dry_run: bool,
    db_lock: Optional[threading.Lock] = None,
) -> None:
    """持久化抓取失败信息"""
    if dry_run:
        return

    status = "failed" if attempt_no >= max_attempts else "retrying"
    err = f"[{attempt_no}/{max_attempts}] {err_summary}"[:1900]
    ts = time.time()

    with db_lock if db_lock is not None else nullcontext():
        existing = conn.execute(
            "SELECT 1 FROM cms_crawl_data_content WHERE id=?", (content_id,)
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE cms_crawl_data_content SET
                   excel_meta=?, crawl_status=?, crawl_error=?, crawl_fail_count=?, updated_at=?
                   WHERE id=?""",
                (excel_meta_json, status, err, attempt_no, ts, content_id),
            )
        else:
            conn.execute(
                """INSERT INTO cms_crawl_data_content(
                   id, description, updated_at, excel_meta, crawl_status, crawl_error, crawl_fail_count)
                   VALUES (?,?,?,?,?,?,?)""",
                (content_id, "", ts, excel_meta_json, status, err, attempt_no),
            )
        conn.commit()


# =============================================================================
# 统计和分类工具函数
# =============================================================================
def classify_error(error_message: str) -> str:
    """分类错误信息"""
    error_lower = str(error_message).lower()

    for category, pattern in ERROR_PATTERNS.items():
        if re.search(pattern, error_lower, re.IGNORECASE):
            return category
    return "other"
