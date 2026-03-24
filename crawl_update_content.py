# -*- coding: utf-8 -*-
"""
从 1.xlsx 读链接，DrissionPage 打开页面，智能提取正文 HTML，写入本地 SQLite。

- 主键 id：优先 Excel「id」，否则「aus_id」。
- 正文提取：若整页为「&lt;pre&gt;{JSON}&lt;/pre&gt;」类接口详情，则解析 JSON 生成结构化 HTML；
  否则 readability → trafilatura → XPath → 启发式 → 智谱 XPath。
- 智谱：从环境变量 ZHIPU_API_KEY 读取；`--no-llm` 可关。SQLITE_DB 等见环境变量。
- 链接列自动识别；每条成功后 commit，日志含「已入库 SQLite」。
- URL 去重表 crawl_url_dedup：同 URL 只爬一次，其它行复用正文写入对应 id（--dup-url-skip 可改为跳过不写）。
- JSON 链接：先 requests GET → …；正文不足再开浏览器。
- excel_meta：整行 Excel 列以 JSON 存入 SQLite，供网页展示。
- 浏览器：不加载图片、静音、eager、单页超时默认 5s；原生 alert/confirm/prompt 自动点「确定」；断连会重启 Chromium。
- 抓取前可滚底拉懒加载（--no-scroll-lazy 可关）。
- 默认按【主域名】轮询调度任务，避免连续狂刷同一站（--sequential-excel 按 Excel 顺序）。
- 成功判定：有效字 ≥ MIN_TEXT_LEN **且**通过正文质量审核（排除整段 JSON、脚本噪声）；否则先 JSON 结构化 / 去 script 再读 / 智谱重组正文。
- 单条失败重试间隔见 --attempt-wait-sec；整表轮次间隔见 --retry-wait-sec（仅与 --loop 同用）。
- 并行：--workers N 时 **单浏览器 + N 个标签页** 同时加载不同链接；SQLite 同连接加锁串行写入（WAL）。
"""
from __future__ import annotations

import argparse
import html as html_module
import json
import os
import queue
import random
import re
import sqlite3
import sys

# 尝试加载 .env 文件
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
import threading
import time
from contextlib import nullcontext
from collections import defaultdict, deque
from typing import Any, Callable, Optional

from loguru import logger
from urllib.parse import urlparse

import pandas as pd
from lxml import etree
from lxml import html as lxml_html
from readability import Document

EXCEL_PATH = os.environ.get("EXCEL_PATH", os.path.join(os.path.dirname(__file__), "1.xlsx"))
SQLITE_DB = os.environ.get("SQLITE_DB", os.path.join(os.path.dirname(__file__), "crawl_local.db"))
# 智谱：从环境变量读取API Key
ZHIPU_API_KEY = os.environ.get("ZHIPU_API_KEY", "")
ZHIPU_MODEL = os.environ.get("ZHIPU_MODEL", "GLM-4-Flash-250414")

COL_MAIN_DOMAIN = "主域名"
# 链接列常见表头（自动识别时会参考）；也可用 --url-col 强制指定
URL_COL_HINTS = (
    "详情页",
    "详情页链接",
    "详情链接",
    "详情地址",
    "Content地址",
    "content地址",
    "content",
    "链接",
    "URL",
    "url",
    "link",
    "页面地址",
    "网页地址",
    "href",
    "源链接",
)

# 正文中「中文+数字+字母」个数至少多少才算抓取成功（不算标点、空格、HTML标签）
MIN_TEXT_LEN = 50
# 审核：至少多少「汉字」，避免 JSON/JS 仅靠字母数字凑满有效字
MIN_CJK_ARTICLE = 22
# 低于此值则仍尝试用浏览器再抓（JSON 先返回了一点内容但不够时）
MIN_VALID_LOOSE = 28
# 失败后重试（同一条任务内连续尝试的次数上限）
MAX_CRAWL_RETRIES_DEFAULT = 0
# 同一条任务两次尝试之间的等待（秒），不是整表大轮间隔
ATTEMPT_WAIT_SEC_DEFAULT = 1
# 配合 --loop：每跑完一整轮 Excel 任务后，休眠多久再读表开跑下一轮（默认 10 分钟）
ROUND_INTERVAL_SEC_DEFAULT = 10
# 给大模型的 HTML 上限（字符）
LLM_HTML_MAX = 85000
# 给大模型的 JSON 文本上限
LLM_JSON_MAX = 120000

JSON_FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}


def init_sqlite(db_path: str) -> None:
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


def migrate_cms_excel_meta_column(conn: sqlite3.Connection) -> None:
    cur = conn.execute("PRAGMA table_info(cms_crawl_data_content)")
    have = {r[1] for r in cur.fetchall()}
    added = False
    for col, ddl in (
        ("excel_meta", "TEXT"),
        ("crawl_status", "TEXT"),
        ("crawl_error", "TEXT"),
        ("crawl_fail_count", "INTEGER DEFAULT 0"),
    ):
        if col not in have:
            conn.execute(
                f"ALTER TABLE cms_crawl_data_content ADD COLUMN {col} {ddl}"
            )
            added = True
            have.add(col)
    if added:
        conn.commit()
        logger.info("已升级表结构：cms_crawl_data_content 新增列（含 crawl_status/crawl_error）")


def valid_text_cjk_digit_alpha(html_or_text: str) -> int:
    """去标签后统计：中文(CJK) + 数字 + 字母（不计标点空格）。"""
    plain = re.sub(r"<[^>]+>", " ", html_or_text or "")
    plain = re.sub(r"\s+", " ", plain)
    return (
        len(re.findall(r"[\u4e00-\u9fff]", plain))
        + len(re.findall(r"\d", plain))
        + len(re.findall(r"[A-Za-z]", plain))
    )


def persist_crawl_failure(
    conn: sqlite3.Connection,
    cid: int,
    excel_meta_json: str,
    err_summary: str,
    attempt_no: int,
    max_attempts: int,
    dry_run: bool,
    db_lock: Optional[threading.Lock] = None,
) -> None:
    if dry_run:
        return
    status = "failed" if attempt_no >= max_attempts else "retrying"
    err = f"[{attempt_no}/{max_attempts}] {err_summary}"[:1900]
    ts = time.time()
    with db_lock if db_lock is not None else nullcontext():
        if conn.execute(
            "SELECT 1 FROM cms_crawl_data_content WHERE id=?", (cid,)
        ).fetchone():
            conn.execute(
                """UPDATE cms_crawl_data_content SET
                   excel_meta=?, crawl_status=?, crawl_error=?, crawl_fail_count=?, updated_at=?
                   WHERE id=?""",
                (excel_meta_json, status, err, attempt_no, ts, cid),
            )
        else:
            conn.execute(
                """INSERT INTO cms_crawl_data_content(
                   id, description, updated_at, excel_meta, crawl_status, crawl_error, crawl_fail_count)
                   VALUES (?,?,?,?,?,?,?)""",
                (cid, "", ts, excel_meta_json, status, err, attempt_no),
            )
        conn.commit()


def reconnect_chromium_if_needed(page: Any, co: Any, err: BaseException) -> Any:
    """浏览器断连等异常时尝试重启实例，避免后续任务连续失败。"""
    msg = (str(err) or "").lower()
    triggers = (
        "disconnected",
        "connection refused",
        "target closed",
        "session",
        "chrome not reachable",
        "websocket",
        "devtools",
        "broken pipe",
        "no such window",
        "invalid session",
    )
    if not any(t in msg for t in triggers):
        return page
    try:
        page.quit()
    except Exception:
        pass
    try:
        from DrissionPage import ChromiumPage

        np = ChromiumPage(addr_or_opts=co)
        apply_auto_accept_browser_dialogs(np)
        logger.warning("检测到浏览器相关异常，已重新创建 ChromiumPage，继续后续任务")
        return np
    except Exception as e2:
        logger.error("重启浏览器失败，后续任务可能仍报错 | {}", e2)
        return page


def _is_page_disconnected_error(e: BaseException) -> bool:
    if type(e).__name__ == "PageDisconnectedError":
        return True
    try:
        from DrissionPage.errors import PageDisconnectedError as _PDE

        return isinstance(e, _PDE)
    except ImportError:
        return False


def apply_auto_accept_browser_dialogs(page: Any) -> None:
    """
    原生 JS 弹窗（alert / confirm / prompt）出现时自动确认，避免卡住。
    注：网页内自定义 div 弹窗不在此列，需另做规则。
    """
    try:
        from DrissionPage._functions.settings import Settings

        Settings.set_auto_handle_alert(True)
    except Exception:
        pass
    try:
        page.set.auto_handle_alert(True, accept=True)
    except Exception:
        pass
    try:
        if hasattr(page, "browser") and hasattr(page.browser, "set"):
            page.browser.set.auto_handle_alert(True, accept=True)
    except Exception:
        pass


def hard_restart_chromium_page(page: Any, co: Any) -> Any:
    """关闭旧实例并新建 ChromiumPage（与页面断连时必须如此）。"""
    try:
        page.quit()
    except Exception:
        pass
    time.sleep(0.5)
    from DrissionPage import ChromiumPage

    np = ChromiumPage(addr_or_opts=co)
    apply_auto_accept_browser_dialogs(np)
    return np


def restart_multitab_browser(
    shared_tabs: list,
    lock: threading.Lock,
    co: Any,
    n_workers: int,
) -> None:
    """
    多标签并行时整实例重启（quit 会关掉所有标签）。
    在 lock 内重建 shared_tabs[0..n_workers-1]。
    """
    from DrissionPage import ChromiumPage

    with lock:
        try:
            if shared_tabs:
                shared_tabs[0].quit()
        except Exception:
            pass
        time.sleep(0.5)
        root = ChromiumPage(addr_or_opts=co)
        apply_auto_accept_browser_dialogs(root)
        shared_tabs.clear()
        shared_tabs.append(root)
        for _ in range(max(0, n_workers - 1)):
            shared_tabs.append(
                root.new_tab(url="about:blank", background=True)
            )


# 单条任务内因断连重启浏览器的次数上限（防死循环）
MAX_BROWSER_RESTART_PER_ROW = 15


def pandas_row_to_excel_meta(row: pd.Series) -> str:
    """Excel 一行 → JSON 字符串（列名→值）。"""
    d: dict[str, Any] = {}
    for k, v in row.items():
        kn = str(k).strip()
        if not kn:
            continue
        if pd.isna(v):
            d[kn] = None
        elif isinstance(v, pd.Timestamp):
            d[kn] = v.isoformat()
        elif isinstance(v, (str, int, float, bool)):
            d[kn] = v
        else:
            try:
                x = v.item()  # numpy scalar
                if isinstance(x, (str, int, float, bool)) or x is None:
                    d[kn] = x
                else:
                    d[kn] = str(v)
            except Exception:
                d[kn] = str(v)
    return json.dumps(d, ensure_ascii=False)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def _clean_header(x) -> str:
        s = str(x).strip()
        s = s.replace("\xa0", "").replace("\u3000", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s

    df.columns = [_clean_header(c) for c in df.columns]
    # 去掉全空列名
    df.columns = [c if c else f"_col_{i}" for i, c in enumerate(df.columns)]
    return df


def _series_has_urls(s: pd.Series, min_hits: int = 2, sample: int = 80) -> bool:
    s = s.dropna().astype(str).str.strip().head(sample)
    if len(s) == 0:
        return False
    hits = s.str.match(r"https?://", case=False, na=False).sum()
    return hits >= min_hits or (
        hits >= 1 and hits >= max(1, len(s) // 10)
    )


def detect_url_columns(df: pd.DataFrame, force_names: Optional[list[str]] = None) -> list[str]:
    """按优先级返回「详情链接」列名列表。"""
    if force_names:
        out = []
        for name in force_names:
            name = name.strip()
            if not name:
                continue
            if name in df.columns:
                out.append(name)
                continue
            for c in df.columns:
                if str(c).strip() == name or str(c).strip().lower() == name.lower():
                    out.append(c)
                    break
        if out:
            return out
        logger.error("指定的 --url-col 在表中不存在: {} | 当前表头: {}", force_names, list(df.columns))
        return []

    scored: list[tuple[int, str]] = []
    for col in df.columns:
        cstr = str(col)
        cl = cstr.lower()
        score = 0
        if _series_has_urls(df[col]):
            score += 50
        for hint in URL_COL_HINTS:
            if hint.lower() == cl or hint == cstr:
                score += 30
                break
            if hint.lower() in cl or hint in cstr:
                score += 15
        if any(k in cstr for k in ("详情", "正文", "公告")):
            score += 8
        if "content" in cl or "链接" in cstr or "url" in cl or "link" in cl or "地址" in cstr:
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


def _parse_id_cell(val) -> Optional[int]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def resolve_content_id(row: pd.Series) -> Optional[int]:
    """优先列名 id，否则 aus_id（大小写/空格变体）。"""
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


def url_fingerprint(url: str) -> str:
    """同一公告 URL 去重用（统一 host 小写、去掉末尾无意义斜杠）。"""
    u = (url or "").strip()
    if not u:
        return ""
    if u.startswith("//"):
        u = "https:" + u
    p = urlparse(u)
    scheme = (p.scheme or "https").lower()
    netloc = (p.netloc or "").lower()
    path = (p.path or "").rstrip("/") or "/"
    q = f"?{p.query}" if p.query else ""
    frag = f"#{p.fragment}" if p.fragment else ""
    return f"{scheme}://{netloc}{path}{q}{frag}"


def pick_url(row: pd.Series, url_columns: list[str]) -> Optional[str]:
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


def domain_key_for_row(row: pd.Series, url: str) -> str:
    if COL_MAIN_DOMAIN in row.index and pd.notna(row[COL_MAIN_DOMAIN]):
        d = str(row[COL_MAIN_DOMAIN]).strip().lower()
        d = re.sub(r"^https?://", "", d).split("/")[0]
        if d:
            return d.replace("www.", "")
    host = urlparse(url).netloc.lower().replace("www.", "")
    return host or "unknown"


def strip_tags_text(s: str) -> str:
    if not s:
        return ""
    t = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", t).strip()


def text_len_from_html(html_fragment: str) -> int:
    if not html_fragment:
        return 0
    t = strip_tags_text(html_fragment)
    return len(t)


def build_domain_round_robin_queue(
    rows: pd.DataFrame,
    url_cols: list[str],
    seed: Optional[int] = None,
) -> tuple[list[tuple[Any, pd.Series]], int, dict[str, int]]:
    """
    按主域名分组后「每轮随机顺序、每域最多取一条」轮询，减少连续请求同一域名。
    同一域内任务顺序随机打散。
    返回 (任务列表 (index, row), 跳过条数, 各域名任务数)。
    """
    if seed is not None:
        random.seed(seed)
    by_dom: dict[str, deque] = defaultdict(deque)
    skip = 0
    for idx, row in rows.iterrows():
        if resolve_content_id(row) is None:
            skip += 1
            continue
        url = pick_url(row, url_cols)
        if not url:
            skip += 1
            continue
        dom = domain_key_for_row(row, url)
        by_dom[dom].append((idx, row))
    counts = {d: len(q) for d, q in by_dom.items()}
    for d in by_dom:
        lst = list(by_dom[d])
        random.shuffle(lst)
        by_dom[d] = deque(lst)
    out: list[tuple[Any, pd.Series]] = []
    while True:
        active = [d for d, q in by_dom.items() if q]
        if not active:
            break
        random.shuffle(active)
        for d in active:
            if by_dom[d]:
                out.append(by_dom[d].popleft())
    return out, skip, counts


def browser_scroll_until_stable(
    page,
    pause: float = 0.55,
    max_rounds: int = 40,
    stable_need: int = 4,
) -> None:
    """
    滚到底并触发常见内部滚动容器，直到 document 高度连续 stable_need 次不再增加。
    用于公告详情等懒加载、分页加载到底部才出现的正文。
    """
    last_h: Optional[float] = None
    stable = 0
    inner_js = r"""
    (function(){
      document.querySelectorAll('*').forEach(function(el){
        try {
          var sh = el.scrollHeight, ch = el.clientHeight;
          if (sh > ch + 100 && el.scrollTop + ch < sh - 20) {
            el.scrollTop = sh;
          }
        } catch(e) {}
      });
    })();
    """
    for rnd in range(max_rounds):
        try:
            page.scroll.to_bottom()
        except Exception:
            try:
                page.run_js(
                    "window.scrollTo(0, Math.max(document.body.scrollHeight,"
                    "document.documentElement.scrollHeight));"
                )
            except Exception:
                break
        time.sleep(pause)
        try:
            page.run_js(inner_js)
        except Exception:
            pass
        time.sleep(max(0.15, pause * 0.35))
        try:
            cur = page.run_js(
                "return Math.max(document.documentElement.scrollHeight||0,"
                "document.body.scrollHeight||0);"
            )
        except Exception:
            cur = 0
        if cur is None:
            cur = 0
        try:
            cur = float(cur)
        except (TypeError, ValueError):
            cur = 0.0
        if last_h is not None and abs(cur - last_h) < 2:
            stable += 1
            if stable >= stable_need:
                break
        else:
            stable = 0
        last_h = cur
    try:
        page.scroll.to_bottom()
        time.sleep(min(1.0, pause + 0.2))
        page.run_js(inner_js)
        time.sleep(0.25)
    except Exception:
        pass


def is_good_main_content(html_fragment: str) -> bool:
    if not html_fragment or len(html_fragment) < 80:
        return False
    vc = valid_text_cjk_digit_alpha(html_fragment)
    if vc < MIN_TEXT_LEN:
        return False
    link_ratio = html_fragment.lower().count("<a ") / max(vc / 80, 1)
    if link_ratio > 8:
        return False
    return True


def extract_readability(html: str) -> str:
    try:
        doc = Document(html)
        s = doc.summary()
        if s and len(s.strip()) > 50:
            return s
    except Exception:
        pass
    return ""


def extract_trafilatura(html: str, page_url: str) -> str:
    try:
        import trafilatura

        # 抽取正文，尽量带结构
        xml = trafilatura.extract(
            html,
            url=page_url or None,
            output_format="xml",
            include_links=True,
            include_tables=True,
            include_images=False,
            favor_precision=True,
        )
        if xml and len(xml.strip()) > 100:
            return f'<div class="trafilatura-extract">{xml}</div>'
        txt = trafilatura.extract(
            html, url=page_url or None, favor_precision=True, include_tables=True
        )
        if txt and valid_text_cjk_digit_alpha(txt) >= MIN_TEXT_LEN:
            esc = html_module.escape(txt)
            return f'<article class="trafilatura-text"><pre>{esc}</pre></article>'
    except Exception:
        pass
    return ""


def _try_parse_json_loose(text: str) -> Optional[Any]:
    """从文本中尽量解析出 JSON 对象。"""
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


def _bidding_json_to_article_html(data: Any) -> str:
    """
    招投标/采购类接口 JSON（含 data.tproject、processList 等）→ 可读 HTML。
    """
    if not isinstance(data, dict):
        return ""
    inner = data.get("data")
    if not isinstance(inner, dict):
        inner = data
    tp = inner.get("tproject")
    if not isinstance(tp, dict):
        tp = {}

    def esc(s: Any) -> str:
        t = "" if s is None else str(s).strip()
        if not t or t.lower() in ("null", "none"):
            return ""
        return html_module.escape(t).replace("\n", "<br/>\n")

    def section(title: str, body: str) -> str:
        if not body:
            return ""
        return (
            f'<section class="json-field"><h2>{html_module.escape(title)}</h2>'
            f"<div>{body}</div></section>"
        )

    parts: list[str] = []
    pname = tp.get("projectName") or inner.get("projectName")
    if pname:
        parts.append(f'<h1 class="json-project-title">{esc(pname)}</h1>')

    low_c = tp.get("lowCapital")
    high_c = tp.get("highCapital")
    cap_txt = ""
    try:
        if low_c is not None and high_c is not None:
            lo, hi = float(low_c), float(high_c)
            if lo or hi:
                cap_txt = f"{lo:g} ～ {hi:g} 万元" if lo != hi else f"{hi:g} 万元"
    except (TypeError, ValueError):
        pass

    blocks = [
        ("项目编号", tp.get("projectNo")),
        ("项目分类", inner.get("projectClassName")),
        ("采购/项目说明", tp.get("projectMessage")),
        ("项目地址", tp.get("projectAddress")),
        ("采购部门", tp.get("purchaseDept")),
        (
            "联系人",
            " ".join(
                x
                for x in (tp.get("purchaserName"), tp.get("mobile"))
                if x and str(x).strip()
            ),
        ),
        ("项目负责人", tp.get("projectManager")),
        ("预算", cap_txt or None),
        (
            "报价起止",
            " ~ ".join(
                x
                for x in (tp.get("projectBjKssj"), tp.get("projectBjJssj"))
                if x and str(x).strip()
            ),
        ),
        ("项目内容 / 采购需求", tp.get("projectContent")),
        ("资质要求", tp.get("qualificationRequier")),
        ("备注", tp.get("projectRemarks")),
    ]
    for title, val in blocks:
        e = esc(val)
        if e:
            parts.append(section(title, e))

    pl = inner.get("processList")
    if isinstance(pl, list) and pl:
        lis = []
        for p in pl:
            if not isinstance(p, dict):
                continue
            nm = esc(p.get("processName"))
            ct = esc(p.get("createTime"))
            if nm:
                lis.append(f"<li>{nm}" + (f" <span>{ct}</span>" if ct else "") + "</li>")
        if lis:
            parts.append(
                '<section class="json-field"><h2>流程节点</h2><ul>'
                + "".join(lis)
                + "</ul></section>"
            )

    def file_block(label: str, obj: Any) -> None:
        if obj is None:
            return
        if isinstance(obj, dict):
            fn = obj.get("fileName") or obj.get("name")
            fn = esc(fn)
            if fn:
                parts.append(section(label, fn))
        elif isinstance(obj, list):
            names = []
            for it in obj:
                if isinstance(it, dict) and it.get("fileName"):
                    names.append(esc(it["fileName"]))
            if names:
                parts.append(
                    section(label, "<ul>" + "".join(f"<li>{n}</li>" for n in names) + "</ul>")
                )

    file_block("采购需求文件", inner.get("cgxqFile"))
    file_block("附件清单", inner.get("fjclFile"))

    shown = {
        "projectName",
        "projectNo",
        "projectMessage",
        "projectAddress",
        "purchaseDept",
        "purchaserName",
        "mobile",
        "projectManager",
        "projectContent",
        "qualificationRequier",
        "projectRemarks",
        "lowCapital",
        "highCapital",
        "projectBjKssj",
        "projectBjJssj",
    }
    extra: list[tuple[str, str]] = []
    for k, v in sorted(tp.items(), key=lambda x: str(x[0])):
        if k in shown or v is None:
            continue
        if isinstance(v, str) and len(v.strip()) > 2:
            if valid_text_cjk_digit_alpha(v) >= 4 or len(v) > 30:
                extra.append((str(k), v.strip()))
    if extra:
        rows = "".join(
            f"<tr><th>{html_module.escape(a)}</th><td>{esc(b)}</td></tr>" for a, b in extra[:25]
        )
        parts.append(
            f'<section class="json-field json-extra"><h2>其他信息</h2><table>{rows}</table></section>'
        )

    if not parts:
        return ""
    return (
        '<article class="from-api-json-detail">'
        + "\n".join(parts)
        + "</article>"
    )


def extract_pre_wrapped_json_as_article(html: str) -> str:
    """
    页面主体为 &lt;pre&gt;{...JSON...}&lt;/pre&gt; 时，解析为结构化正文 HTML。
    """
    if not html or len(html) < 80:
        return ""
    best: Optional[Any] = None
    best_len = 0
    for m in re.finditer(r"<pre[^>]*>([\s\S]*?)</pre>", html, re.I):
        raw = m.group(1)
        raw = re.sub(r"<[^>]+>", " ", raw)
        raw = raw.strip()
        if len(raw) < 50:
            continue
        data = _try_parse_json_loose(raw)
        if data is not None and len(raw) > best_len:
            best_len = len(raw)
            best = data
    if best is None:
        return ""
    article = _bidding_json_to_article_html(best)
    if article and valid_text_cjk_digit_alpha(article) >= 12:
        return article
    return ""


STATIC_XPATHS: list[str] = [
    "//article",
    "//*[@id='content' or @id='Content' or @id='mainContent' or @id='articleContent']",
    "//*[@class='article-content' or @class='article_content' or contains(@class,'article-detail')]",
    "//*[@class='detail' or contains(@class,'detail-content') or contains(@class,'detail_content')]",
    "//div[contains(@class,'zw') or contains(@class,'news_content') or contains(@class,'news-content')]",
    "//div[contains(@class,'ggnr') or contains(@class,'announce') or contains(@class,'notice-body')]",
    "//div[contains(@class,'TRS_Editor') or @id='TRS_AUTOADD']",
    "//div[contains(@class,'main') and string-length(normalize-space(.))>500]",
    "//div[contains(@class,'article')]",
    "//td[contains(@class,'content') or contains(@class,'article')]",
]


def extract_by_xpaths(html: str, xpaths: list[str]) -> str:
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


def extract_largest_text_div(html: str) -> str:
    """选 body 下文本量最大、链接占比相对低的 div（启发式）。"""
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
            # 分數：文本長 - 懲罰過多鏈接與過深嵌套
            depth = len(list(div.iterancestors()))
            score = len(text) - 15 * min(a_count, 50) - 2 * max(0, depth - 15)
            if score > best_score:
                best_score = score
                best_html = etree.tostring(div, encoding="unicode", method="html")
        except Exception:
            continue
    return best_html if is_good_main_content(best_html) else ""


def apply_single_xpath(html: str, xpath: str) -> str:
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


def try_fetch_json(url: str) -> tuple[Optional[Any], str, int]:
    """
    GET 链接：JSON 则 (对象, 片段, http状态码)；否则 (None, "", 状态码)。
    404/410 明确返回，供上层「不再多次重试」。
    """
    try:
        import requests

        requests.packages.urllib3.disable_warnings()
    except Exception:
        return None, "", 0
    try:
        r = requests.get(
            url, headers=JSON_FETCH_HEADERS, timeout=40, allow_redirects=True
        )
        sc = int(r.status_code)
        if sc in (404, 410):
            return None, "", sc
        r.raise_for_status()
        text = (r.text or "").lstrip("\ufeff").strip()
        if not text or text[0] not in "{[":
            return None, "", sc
        data = json.loads(text)
        sample = text if len(text) <= LLM_JSON_MAX else text[:LLM_JSON_MAX]
        return data, sample, sc
    except Exception as e:
        sc = 0
        resp = getattr(e, "response", None)
        if resp is not None:
            try:
                sc = int(getattr(resp, "status_code", 0) or 0)
            except (TypeError, ValueError):
                sc = 0
        if sc in (404, 410):
            return None, "", sc
        logger.debug("非 JSON 或请求失败: {} | {}", url[:70], e)
        return None, "", sc


def json_path_get(root: Any, path: str) -> Any:
    """
    从根对象按路径取值。路径示例：data.content、result.list[0].htmlBody
    """
    path = (path or "").strip().strip("$").strip(".")
    if not path:
        return root
    cur: Any = root
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
            if not isinstance(cur, list) or idx < 0 or idx >= len(cur):
                return None
            cur = cur[idx]
            i = j + 1
        else:
            j = i
            while j < n and path[j] not in ".[":
                j += 1
            key = path[i:j].strip().strip('"').strip("'")
            if not isinstance(cur, dict):
                return None
            if key not in cur:
                for k in cur:
                    if str(k).strip() == key:
                        cur = cur[k]
                        break
                else:
                    return None
            else:
                cur = cur[key]
            i = j
    return cur


def stringify_json_sample(data: Any) -> str:
    try:
        s = json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        s = str(data)
    return s[:LLM_JSON_MAX]


def guess_longest_html_in_json(obj: Any, depth: int = 0, max_depth: int = 18) -> str:
    """递归找最长的、像 HTML 的字符串。"""
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
    """常见 API 字段名试探。"""
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


def get_learned_json_path(
    conn: sqlite3.Connection, domain: str, db_lock: Optional[threading.Lock] = None
) -> Optional[str]:
    with db_lock if db_lock is not None else nullcontext():
        cur = conn.execute(
            "SELECT json_path FROM domain_json_html_path WHERE domain = ?", (domain,)
        )
        row = cur.fetchone()
    return row[0] if row else None


def save_learned_json_path(
    conn: sqlite3.Connection,
    domain: str,
    json_path: str,
    sample_url: str,
    db_lock: Optional[threading.Lock] = None,
) -> None:
    with db_lock if db_lock is not None else nullcontext():
        conn.execute(
            """INSERT INTO domain_json_html_path(domain, json_path, sample_url, updated_at)
               VALUES (?,?,?,?)
               ON CONFLICT(domain) DO UPDATE SET
               json_path=excluded.json_path, sample_url=excluded.sample_url,
               updated_at=excluded.updated_at""",
            (domain, json_path, sample_url, time.time()),
        )
        conn.commit()


def ask_llm_json_html_path(api_key: str, page_url: str, json_snippet: str) -> str:
    """大模型返回 JSON 里正文 HTML 的路径，如 data.result.content。"""
    try:
        from zhipuai import ZhipuAI
    except Exception as e:
        logger.warning(
            "zhipuai 不可用（{}: {}）跳过 JSON 路径 LLM | 解释器: {} | 诊断: python test_zhipuai_env.py",
            type(e).__name__,
            e,
            sys.executable,
        )
        return ""

    client = ZhipuAI(api_key=api_key)
    prompt = f"""你是接口分析专家。下面是某招投标/政府采购相关接口返回的 JSON（可能已截断）。

接口 URL：{page_url}

请找出：存放「公告正文」的那段 **HTML 字符串** 在 JSON 里的访问路径（从根对象一层层下去）。

只输出一个 JSON 对象，不要 markdown：
{{"json_path":"路径","note":"可选"}}

路径格式要求（须能被程序按段解析）：
- 只用英文键名、数字下标，形如：data.content 或 result.items[0].htmlBody 或 list[0].detail.ggText
- 键之间用点号 . 连接，数组用 [0]、[1]，不要写成 /aa/bb 或 $..xx
- 若没有任何字段是较长的 HTML 字符串，json_path 填空字符串"""

    raw = ""
    try:
        resp = client.chat.completions.create(
            model=ZHIPU_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": prompt + "\n\n--- JSON ---\n" + json_snippet[:LLM_JSON_MAX],
                }
            ],
            response_format={"type": "json_object"},
            max_tokens=400,
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = json.loads(raw)
        if isinstance(data, dict):
            p = (
                data.get("json_path")
                or data.get("path")
                or data.get("jsonPath")
                or ""
            )
            return str(p).strip()
    except Exception:
        logger.exception("智谱 json_path 解析失败")
    m = re.search(
        r'"json_path"\s*:\s*"((?:[^"\\]|\\.)*)"', raw or "", re.I
    )
    if m:
        return m.group(1).replace('\\"', '"').strip()
    return ""


def extract_main_from_json(
    data: Any,
    json_text_for_llm: str,
    page_url: str,
    domain: str,
    conn: sqlite3.Connection,
    zhipu_key: str,
    domain_json_llm_asked: set[str],
    force_relearn: bool,
    db_lock: Optional[threading.Lock] = None,
    domain_lock: Optional[threading.Lock] = None,
) -> tuple[str, str]:
    """从 JSON 对象中取正文 HTML，失败返回 ("", "")。"""
    if data is None:
        return "", ""

    def ok_html(h: str) -> bool:
        return bool(
            h and (valid_text_cjk_digit_alpha(h) >= MIN_VALID_LOOSE or len(h) > 400)
        )

    if not force_relearn:
        learned = get_learned_json_path(conn, domain, db_lock=db_lock)
        if learned:
            v = json_path_get(data, learned)
            if isinstance(v, str) and ok_html(v):
                return v, "json_path_cached"
            if isinstance(v, dict):
                inner = try_common_json_html_keys(v) or guess_longest_html_in_json(v)
                if ok_html(inner):
                    return inner, "json_path_cached_nested"

    h = try_common_json_html_keys(data)
    if ok_html(h):
        return h, "json_key_heuristic"

    h = guess_longest_html_in_json(data)
    if ok_html(h):
        return h, "json_html_scan"

    art = _bidding_json_to_article_html(data)
    if art:
        vca = valid_text_cjk_digit_alpha(art)
        if vca >= MIN_TEXT_LEN:
            return art, "json_detail_synthesized"
        if vca >= MIN_VALID_LOOSE:
            return art, "json_detail_synthesized_loose"

    do_json_llm = False
    if zhipu_key:
        if domain_lock:
            with domain_lock:
                if domain not in domain_json_llm_asked:
                    domain_json_llm_asked.add(domain)
                    do_json_llm = True
        elif domain not in domain_json_llm_asked:
            domain_json_llm_asked.add(domain)
            do_json_llm = True
    if do_json_llm:
        logger.info("[LLM-JSON] 域名「{}」请求正文 HTML 的 json_path（本批一次）", domain)
        snippet = json_text_for_llm or stringify_json_sample(data)
        jp = ask_llm_json_html_path(zhipu_key, page_url, snippet)
        if jp:
            v = json_path_get(data, jp)
            if isinstance(v, str) and v.strip():
                if ok_html(v):
                    save_learned_json_path(conn, domain, jp, page_url, db_lock=db_lock)
                    return v, "json_llm_path"
                save_learned_json_path(conn, domain, jp, page_url, db_lock=db_lock)
                logger.warning("[LLM-JSON] 路径已缓存但正文偏短: {} → {}", jp, len(v))
                return v, "json_llm_path_weak"
            logger.warning("[LLM-JSON] 路径无效或类型非字符串: {}", jp)

    learned2 = get_learned_json_path(conn, domain, db_lock=db_lock)
    if learned2:
        v = json_path_get(data, learned2)
        if isinstance(v, str) and v.strip():
            return v, "json_path_retry"
    return "", ""


def get_learned_xpath(
    conn: sqlite3.Connection, domain: str, db_lock: Optional[threading.Lock] = None
) -> Optional[str]:
    with db_lock if db_lock is not None else nullcontext():
        cur = conn.execute(
            "SELECT xpath FROM domain_learned_xpath WHERE domain = ?", (domain,)
        )
        row = cur.fetchone()
    return row[0] if row else None


def save_learned_xpath(
    conn: sqlite3.Connection,
    domain: str,
    xpath: str,
    sample_url: str,
    db_lock: Optional[threading.Lock] = None,
) -> None:
    with db_lock if db_lock is not None else nullcontext():
        conn.execute(
            """INSERT INTO domain_learned_xpath(domain, xpath, sample_url, updated_at)
               VALUES (?,?,?,?)
               ON CONFLICT(domain) DO UPDATE SET
               xpath=excluded.xpath, sample_url=excluded.sample_url, updated_at=excluded.updated_at""",
            (domain, xpath, sample_url, time.time()),
        )
        conn.commit()


def ask_llm_xpath(api_key: str, page_url: str, html_snippet: str) -> str:
    """返回 XPath 字符串，失败返回空。"""
    try:
        from zhipuai import ZhipuAI
    except Exception as e:
        logger.warning(
            "zhipuai 不可用（{}: {}）跳过 LLM | 解释器: {} | 诊断: python test_zhipuai_env.py",
            type(e).__name__,
            e,
            sys.executable,
        )
        return ""

    client = ZhipuAI(api_key=api_key)
    snippet = html_snippet[:LLM_HTML_MAX]
    prompt = f"""你是网页结构分析专家。下面是招投标/政府采购「详情页」HTML（已截断）。

页面完整 URL：{page_url}

请找出包裹「公告正文」的单个 DOM 节点（含招标人、项目、标段等实质内容，不要导航/页脚/侧栏）。

只输出一个 JSON 对象，不要 markdown，格式：
{{"xpath":"此处填 XPath 1.0，在 lxml 里对整页 HTML 能选中 1 个节点","note":"可选一句话"}}

规则：
- xpath 必须合法，例如 //div[@id='xxx'] 或 //div[contains(@class,'article')]
- 若页面是 iframe 动态加载你无法判断，xpath 填 ""
- 不要选中整个 body，尽量精确到正文容器"""

    raw = ""
    try:
        resp = client.chat.completions.create(
            model=ZHIPU_MODEL,
            messages=[{"role": "user", "content": prompt + "\n\n--- HTML 开始 ---\n" + snippet}],
            response_format={"type": "json_object"},
            max_tokens=500,
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = json.loads(raw)
        if isinstance(data, dict):
            xp = data.get("xpath") or data.get("XPath") or ""
            return str(xp).strip()
    except Exception:
        logger.exception("智谱 XPath 请求解析失败")
    m = re.search(r'"xpath"\s*:\s*"((?:[^"\\]|\\.)*)"', raw, re.I)
    if m:
        return m.group(1).replace("\\\"", '"').strip()
    return ""


def try_parse_json_object_from_messy_html(html: str) -> Optional[dict]:
    """从 HTML 或纯文本中抠出最外层 JSON 对象。"""
    for blob in (
        re.sub(r"<[^>]+>", " ", html or ""),
        html or "",
    ):
        t = html_module.unescape(blob)
        t = re.sub(r"\s+", " ", t).strip()
        i = t.find("{")
        j = t.rfind("}")
        if i < 0 or j <= i:
            continue
        try:
            obj = json.loads(t[i : j + 1])
            if isinstance(obj, dict) and len(obj) >= 1:
                return obj
        except Exception:
            continue
    return None


def looks_like_json_api_blob(html: str) -> bool:
    """整段主要是接口 JSON（未转成可读 HTML）。"""
    plain = re.sub(r"<[^>]+>", "", html or "")
    plain = re.sub(r"\s+", "", plain)
    if len(plain) < 100:
        return False
    d = try_parse_json_object_from_messy_html(html)
    if not isinstance(d, dict):
        return False
    if "code" in d and "data" in d and "msg" in d:
        return True
    if plain.startswith("{") and plain.count('":"') + plain.count('":') >= 10:
        return True
    return False


def looks_like_javascript_noise(html: str) -> bool:
    """提取结果主要是 jQuery/脚本而非公告正文。"""
    low = (html or "").lower()
    plain = re.sub(r"<[^>]+>", " ", html or "")
    cjk = len(re.findall(r"[\u4e00-\u9fff]", plain))
    if cjk >= 120:
        return False
    hits = 0
    for p in (
        r"\$\s*\(",
        r"\bfunction\s*\(",
        r"\.change\s*\(",
        r"\.val\s*\(",
        r"\bvar\s+\w+\s*=",
        r"=>",
        r"document\.",
        r"window\.",
        r"addeventlistener",
        r"getelementbyid",
    ):
        if re.search(p, low):
            hits += 1
    if hits >= 3 or (hits >= 2 and cjk < 55):
        return True
    if low.count("<script") >= 1 and cjk < 75:
        return True
    return False


def count_cjk(html_or_text: str) -> int:
    plain = re.sub(r"<[^>]+>", " ", html_or_text or "")
    return len(re.findall(r"[\u4e00-\u9fff]", plain))


def is_quality_article_html(html: str) -> bool:
    """入库前：像真人可读公告，而非 JSON/脚本。"""
    if not html or len(html.strip()) < 80:
        return False
    if count_cjk(html) < MIN_CJK_ARTICLE:
        return False
    if looks_like_json_api_blob(html):
        return False
    if looks_like_javascript_noise(html):
        return False
    return True


def page_indicates_404_or_missing(html: str) -> bool:
    """
    浏览器打开的页面是否为 404 / 不存在 / 已删除类（且通常正文很短）。
    """
    if not html or len(html) < 40:
        return False
    low = html.lower()
    if re.search(r"<title[^>]*>[^<]{0,120}</title>", html, re.I):
        m = re.search(r"<title[^>]*>([^<]{0,120})</title>", html, re.I)
        if m and re.search(
            r"404|4\s*0\s*4|not\s*found|不存在|找不到|无法找到|页面.?丢失|已删除|失效",
            m.group(1),
            re.I,
        ):
            return True
    plain = re.sub(r"<[^>]+>", " ", html[:8000])
    plain = re.sub(r"\s+", " ", plain).strip()
    if len(plain) > 900:
        return False
    if re.search(
        r"\b404\b|not\s+found|page\s+not\s+found|页面不存在|找不到页面|"
        r"无法访问|地址错误|链接无效|该页面不存在|404\s*错误",
        plain,
        re.I,
    ):
        return True
    return False


def classify_extraction_source(method: str) -> str:
    """
    日志用：正文是否经智谱大模型参与（XPath / json_path / 正文重组）。
    """
    m = method or ""
    if re.search(r"llm_xpath|json_llm|llm_article", m, re.I):
        return "大模型(智谱)"
    return "规则算法"


def strip_scripts_styles_html(html: str) -> str:
    try:
        tree = lxml_html.fromstring(html or "")
        for el in tree.xpath("//script|//style|//noscript"):
            p = el.getparent()
            if p is not None:
                p.remove(el)
        return etree.tostring(tree, encoding="unicode", method="html")
    except Exception:
        return re.sub(
            r"(?is)<(script|style|noscript)\b[^>]*>.*?</\1>",
            "",
            html or "",
        )


def ask_llm_reconstruct_article(api_key: str, page_url: str, fragment: str) -> str:
    """从 JSON/脚本/杂乱 HTML 中整理出招标采购可读正文 HTML。"""
    try:
        from zhipuai import ZhipuAI
    except Exception as e:
        logger.debug("智谱不可用，跳过正文重组 | {}", e)
        return ""
    frag = (fragment or "")[:LLM_HTML_MAX]
    if len(frag) < 30:
        return ""
    client = ZhipuAI(api_key=api_key)
    prompt = f"""页面 URL：{page_url}

以下是从详情页抓到的片段，可能是：**接口 JSON**、**JavaScript**、或结构混乱的 HTML。
请只整理输出「采购/招标/比价公告」里供应商需要阅读的**实质正文**（项目概况、需求说明、资质、时间、联系方式等）。

规则：
1. 输出**一段 HTML**，仅用 h2、h3、p、ul、li、table、strong；禁止 script、style、不要输出整段 JSON 花括号格式。
2. 若本质是 JSON，把有意义的字段写成小节标题+段落，不要保留 "key":"value" 机器格式。
3. 若几乎没有可用招标正文，只输出：<p></p>

只输出 HTML，不要 markdown 代码块。

---- 片段（截断） ----
{frag}
---- 结束 ----"""
    try:
        resp = client.chat.completions.create(
            model=ZHIPU_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=4096,
        )
        raw = (resp.choices[0].message.content or "").strip()
        raw = re.sub(r"^```(?:html)?\s*", "", raw, flags=re.I)
        raw = re.sub(r"\s*```\s*$", "", raw)
        if "<" in raw and ">" in raw and len(raw) > 50:
            return raw[:800000]
    except Exception:
        logger.exception("智谱正文重组失败")
    return ""


def refine_extracted_to_quality_article(
    html: str,
    page_url: str,
    zhipu_key: str,
    no_llm: bool,
    base_method: str,
) -> tuple[str, str]:
    """
    审核并尽量修正正文。返回 (html, method_suffix)。
    """
    h = html or ""
    m = base_method or "unknown"

    if looks_like_json_api_blob(h):
        d = try_parse_json_object_from_messy_html(h)
        if d:
            syn = _bidding_json_to_article_html(d)
            if syn and valid_text_cjk_digit_alpha(syn) >= MIN_VALID_LOOSE:
                h, m = syn, f"{m}|json_to_html"
                logger.info("[正文审核] 接口 JSON → 结构化 HTML | {}", m)

    if is_quality_article_html(h) and valid_text_cjk_digit_alpha(h) >= MIN_TEXT_LEN:
        return h, m

    h2 = strip_scripts_styles_html(h)
    if h2 and h2 != h:
        frag = extract_readability(h2) or extract_trafilatura(h2, page_url)
        if frag and valid_text_cjk_digit_alpha(frag) >= MIN_TEXT_LEN:
            if is_quality_article_html(frag):
                logger.info("[正文审核] 去 script 后重新抽取通过 | {}", m)
                return frag, f"{m}|denoise_extract"

    need_llm = (
        not is_quality_article_html(h)
        or looks_like_javascript_noise(h)
        or looks_like_json_api_blob(h)
    )
    if need_llm and zhipu_key and not no_llm:
        logger.info("[正文审核] 调用智谱重组正文 | {}", page_url[:80])
        llm_html = ask_llm_reconstruct_article(zhipu_key, page_url, h)
        vc_llm = valid_text_cjk_digit_alpha(llm_html or "")
        if vc_llm >= MIN_TEXT_LEN and is_quality_article_html(llm_html):
            return llm_html, f"{m}|llm_article"
        if (
            vc_llm >= MIN_TEXT_LEN
            and count_cjk(llm_html) >= max(MIN_CJK_ARTICLE, count_cjk(h) + 10)
            and not looks_like_javascript_noise(llm_html)
            and not looks_like_json_api_blob(llm_html)
        ):
            return llm_html, f"{m}|llm_article_loose"

    return h, m


def smart_extract_main_html(
    html: str,
    page_url: str,
    domain: str,
    conn: sqlite3.Connection,
    zhipu_key: str,
    domain_llm_asked: set[str],
    force_relearn: bool,
    db_lock: Optional[threading.Lock] = None,
    domain_lock: Optional[threading.Lock] = None,
) -> tuple[str, str]:
    """
    返回 (html_fragment, method_label)
    """
    if not html or len(html) < 200:
        return "", "empty"

    pre_art = extract_pre_wrapped_json_as_article(html)
    if pre_art and valid_text_cjk_digit_alpha(pre_art) >= MIN_TEXT_LEN:
        return pre_art, "pre_json_detail"

    # 1) 库里已有 XPath（同主域名复用，省规则遍历）
    if not force_relearn:
        learned = get_learned_xpath(conn, domain, db_lock=db_lock)
        if learned:
            frag = apply_single_xpath(html, learned)
            if is_good_main_content(frag):
                return frag, "learned_xpath"

    chain: list[tuple[str, Callable[[], str]]] = [
        ("readability", lambda: extract_readability(html)),
        ("trafilatura", lambda: extract_trafilatura(html, page_url)),
        ("static_xpath", lambda: extract_by_xpaths(html, STATIC_XPATHS)),
        ("largest_div", lambda: extract_largest_text_div(html)),
    ]

    for name, fn in chain:
        frag = fn()
        if is_good_main_content(frag):
            return frag, name

    do_llm_xpath = False
    if zhipu_key:
        if domain_lock:
            with domain_lock:
                if domain not in domain_llm_asked:
                    domain_llm_asked.add(domain)
                    do_llm_xpath = True
        elif domain not in domain_llm_asked:
            domain_llm_asked.add(domain)
            do_llm_xpath = True
    if do_llm_xpath:
        logger.info("[LLM] 主域名「{}」首次请求 XPath（本批只问一次）", domain)
        xp = ask_llm_xpath(zhipu_key, page_url, html)
        if xp:
            frag = apply_single_xpath(html, xp)
            if is_good_main_content(frag):
                save_learned_xpath(conn, domain, xp, page_url, db_lock=db_lock)
                return frag, "llm_xpath"
            save_learned_xpath(conn, domain, xp, page_url, db_lock=db_lock)
            logger.warning(
                "[LLM] XPath 已写入库，当前页效果一般: {}…", xp[:100]
            )

    learned2 = get_learned_xpath(conn, domain, db_lock=db_lock)
    if learned2:
        frag = apply_single_xpath(html, learned2)
        if frag and valid_text_cjk_digit_alpha(frag) >= MIN_VALID_LOOSE:
            return frag, "learned_xpath_retry"

    frag = extract_readability(html) or extract_trafilatura(html, page_url)
    if frag:
        return frag, "weak_fallback"
    # 最后截断 body
    try:
        tree = lxml_html.fromstring(html)
        body = tree.find(".//body")
        if body is not None:
            frag = etree.tostring(body, encoding="unicode", method="html")
            return frag[:500000], "body_trunc"
    except Exception:
        pass
    return (html[:500000] if html else ""), "raw_trunc"


def build_chromium_options(args: Any) -> Any:
    from DrissionPage import ChromiumOptions

    co = ChromiumOptions()
    if args.headless:
        co.headless()
    co.set_argument("--no-sandbox")
    co.set_argument("--disable-gpu")
    pt = max(2.0, min(120.0, float(args.page_timeout)))
    co.set_timeouts(
        page_load=pt,
        base=min(30.0, pt + 5.0),
        script=max(12.0, pt + 8.0),
    )
    co.set_load_mode(args.browser_load_mode)
    if not args.browser_load_images:
        co.no_imgs(True)
    co.mute(True)
    for _arg in (
        "--disable-extensions",
        "--disable-background-networking",
        "--disable-sync",
        "--disable-translate",
        "--disable-default-apps",
        "--disable-dev-shm-usage",
    ):
        co.set_argument(_arg)
    return co


def crawl_process_one_row(
    idx: int,
    row: Any,
    cur_n: int,
    total_rows: int,
    url_cols: list[str],
    args: Any,
    conn: sqlite3.Connection,
    sql_success: str,
    page: Any,
    co: Any,
    pt: float,
    use_url_dedup: bool,
    zhipu_key: str,
    domain_llm_asked: set[str],
    domain_json_llm_asked: set[str],
    domains_cache_cleared: set[str],
    db_lock: Optional[threading.Lock],
    domain_lock: Optional[threading.Lock],
    multitab: Optional[tuple] = None,
) -> tuple[Any, dict[str, int]]:
    """
    处理单行抓取。multitab=(shared_tabs, lock, co, n_workers, wid) 时为多标签并行中本 worker 独占的标签。
    返回 (page/tab 引用, 计数增量)。
    """
    L = db_lock if db_lock is not None else nullcontext()
    DL = domain_lock if domain_lock is not None else nullcontext()
    st = {
        "ok": 0,
        "fail": 0,
        "ok_crawl": 0,
        "ok_json": 0,
        "ok_reuse": 0,
        "skip_dup": 0,
        "skip_ok": 0,
    }

    cid = resolve_content_id(row)
    if cid is None:
        logger.warning("跳过：无 id/aus_id（Excel第{}行）", idx + 2)
        st["fail"] = 1
        return page, st
    url = pick_url(row, url_cols)
    if not url:
        logger.warning("跳过：无链接 | id={} | Excel第{}行", cid, idx + 2)
        st["fail"] = 1
        return page, st
    dom = domain_key_for_row(row, url)
    pct = (100.0 * cur_n / total_rows) if total_rows else 0.0
    logger.info(
        "━━━━ 进度 {}/{} ({:.1f}%) ━━━━ [{}] ━━ Excel第{}行 id={}",
        cur_n,
        total_rows,
        pct,
        dom,
        idx + 2,
        cid,
    )
    excel_meta_json = pandas_row_to_excel_meta(row)

    if not args.force_recrawl and not args.dry_run:
        with L:
            prev = conn.execute(
                "SELECT description FROM cms_crawl_data_content WHERE id = ?",
                (cid,),
            ).fetchone()
        if prev and prev[0]:
            pv = valid_text_cjk_digit_alpha(prev[0])
            if pv >= MIN_TEXT_LEN:
                if not args.dry_run:
                    with L:
                        conn.execute(
                            """UPDATE cms_crawl_data_content SET
                               excel_meta = ?, crawl_status = 'ok', crawl_error = NULL,
                               crawl_fail_count = 0, updated_at = ?
                               WHERE id = ?""",
                            (excel_meta_json, time.time(), cid),
                        )
                        conn.commit()
                logger.info(
                    "【跳过】结果库已有达标正文，不抓取 | id={} | 库表=cms_crawl_data_content | "
                    "有效字={}/≥{} | 已仅更新Excel元数据 | {}/{}",
                    cid,
                    pv,
                    MIN_TEXT_LEN,
                    cur_n,
                    total_rows,
                )
                st["ok"] = 1
                st["skip_ok"] = 1
                time.sleep(args.interval)
                return page, st

    url_key = url_fingerprint(url)
    if use_url_dedup and url_key and not args.dry_run:
        with L:
            ex = conn.execute(
                "SELECT first_content_id FROM crawl_url_dedup WHERE url_key = ?",
                (url_key,),
            ).fetchone()
        if ex:
            first_id = ex[0]
            if first_id == cid:
                with L:
                    dr = conn.execute(
                        "SELECT description FROM cms_crawl_data_content WHERE id = ?",
                        (cid,),
                    ).fetchone()
                if dr and dr[0] and valid_text_cjk_digit_alpha(dr[0]) >= MIN_TEXT_LEN:
                    if not args.dry_run:
                        with L:
                            conn.execute(
                                """UPDATE cms_crawl_data_content SET
                                   excel_meta = ?, crawl_status = 'ok', crawl_error = NULL,
                                   crawl_fail_count = 0, updated_at = ? WHERE id = ?""",
                                (excel_meta_json, time.time(), cid),
                            )
                            conn.commit()
                    logger.info(
                        "【跳过】结果库同id已有达标正文(URL去重同条) | id={} | 库表=cms_crawl_data_content | "
                        "仅同步Excel | {}/{}",
                        cid,
                        cur_n,
                        total_rows,
                    )
                    st["ok"] = 1
                    st["skip_ok"] = 1
                    return page, st
            with L:
                dr = conn.execute(
                    "SELECT description FROM cms_crawl_data_content WHERE id = ?",
                    (first_id,),
                ).fetchone()
            reuse_html = dr[0] if dr else None
            if not reuse_html or valid_text_cjk_digit_alpha(reuse_html) < MIN_VALID_LOOSE:
                with L:
                    conn.execute(
                        "DELETE FROM crawl_url_dedup WHERE url_key = ?", (url_key,)
                    )
                    conn.commit()
            elif reuse_html:
                if args.dup_url_skip:
                    logger.warning(
                        "【跳过】重复URL不写库(--dup-url-skip) | 当前id={} | 结果库已有同链接首条id={} | {}/{}",
                        cid,
                        first_id,
                        cur_n,
                        total_rows,
                    )
                    st["skip_dup"] = 1
                    return page, st
                if not args.dry_run:
                    with L:
                        conn.execute(
                            sql_success,
                            (cid, reuse_html, time.time(), excel_meta_json),
                        )
                        conn.commit()
                ntxt = valid_text_cjk_digit_alpha(reuse_html)
                logger.success(
                    "【已入库】复用结果库已有正文(同URL未再爬) | 写入id={} | 正文来源首抓id={} | "
                    "正文来源类型: 历史入库(非本次抓取) | 有效字={} | {}/{} | {}",
                    cid,
                    first_id,
                    ntxt,
                    cur_n,
                    total_rows,
                    url[:65] + ("…" if len(url) > 65 else ""),
                )
                st["ok"] = 1
                st["ok_reuse"] = 1
                time.sleep(args.interval)
                return page, st

    force_dom = False
    if args.force_relearn_domain:
        with DL:
            if dom not in domains_cache_cleared:
                domains_cache_cleared.add(dom)
                _clear_dom = True
            else:
                _clear_dom = False
        if _clear_dom:
            with L:
                conn.execute(
                    "DELETE FROM domain_learned_xpath WHERE domain = ?", (dom,)
                )
                conn.execute(
                    "DELETE FROM domain_json_html_path WHERE domain = ?", (dom,)
                )
                conn.commit()
            with DL:
                domain_llm_asked.discard(dom)
                domain_json_llm_asked.discard(dom)
            force_dom = True

    max_retries = 1 if (args.dry_run or args.no_crawl_retry) else max(
        1, int(args.max_crawl_retries)
    )
    attempt_wait = 0 if args.dry_run else max(0, int(args.attempt_wait_sec))

    succeeded = False
    last_err = ""
    main_html, method, used_browser = "", "", False
    got_json_first = False

    attempt = 0
    row_disconnect_recoveries = 0
    while attempt < max_retries:
        attempt += 1
        if multitab is not None:
            stabs, _, _, _, mwid = multitab
            page = stabs[mwid]
        stop_retry = False
        try:
            main_html, method = "", ""
            used_browser = False
            got_json_first = False
            last_page_html = ""
            http_code = 0

            if not args.no_json_fetch:
                jd, jtxt, http_code = try_fetch_json(url)
                if http_code in (404, 410):
                    last_err = (
                        f"HTTP {http_code} 资源不存在，有效字不足(<{MIN_TEXT_LEN})，本条不再重试"
                    )
                    stop_retry = True
                elif jd is not None:
                    got_json_first = True
                    main_html, method = extract_main_from_json(
                        jd,
                        jtxt,
                        url,
                        dom,
                        conn,
                        zhipu_key,
                        domain_json_llm_asked,
                        force_dom,
                        db_lock=db_lock,
                        domain_lock=domain_lock,
                    )
            if not stop_retry:
                vc_pre = valid_text_cjk_digit_alpha(main_html or "")
                if (not main_html) or (vc_pre < MIN_TEXT_LEN):
                    br = 0
                    html = ""
                    while True:
                        try:
                            page.get(url, timeout=pt)
                            time.sleep(args.wait)
                            if not args.no_scroll_lazy:
                                logger.debug("滚底加载懒加载正文…")
                                browser_scroll_until_stable(
                                    page,
                                    pause=args.scroll_pause,
                                    max_rounds=args.scroll_max_rounds,
                                )
                            html = page.html or ""
                            last_page_html = html
                            break
                        except Exception as be:
                            if (
                                _is_page_disconnected_error(be)
                                and br < MAX_BROWSER_RESTART_PER_ROW
                            ):
                                br += 1
                                logger.warning(
                                    "PageDisconnected / 与页面连接已断开，正在重启 Chromium 后重试打开 | "
                                    "id={} | 本轮第{}/{} 次",
                                    cid,
                                    br,
                                    MAX_BROWSER_RESTART_PER_ROW,
                                )
                                if multitab is not None:
                                    st, lk, c, nw, wi = multitab
                                    restart_multitab_browser(st, lk, c, nw)
                                    page = st[wi]
                                else:
                                    page = hard_restart_chromium_page(page, co)
                                continue
                            raise
                    main_html, method = smart_extract_main_html(
                        html,
                        url,
                        dom,
                        conn,
                        zhipu_key,
                        domain_llm_asked,
                        force_dom,
                        db_lock=db_lock,
                        domain_lock=domain_lock,
                    )
                    used_browser = True

                vc = valid_text_cjk_digit_alpha(main_html or "")
                if vc >= MIN_TEXT_LEN:
                    main_html, method = refine_extracted_to_quality_article(
                        main_html,
                        url,
                        zhipu_key,
                        args.no_llm,
                        method,
                    )
                    vc = valid_text_cjk_digit_alpha(main_html or "")
                    if is_quality_article_html(main_html) and vc >= MIN_TEXT_LEN:
                        succeeded = True
                        break
                    last_err = (
                        f"正文质量审核未通过；有效字={vc}；"
                        f"初检来源={classify_extraction_source(method)}；路径={method}；"
                        f"需智谱请在文件顶部填写 ZHIPU_API_KEY 且勿 --no-llm"
                    )
                else:
                    last_err = (
                        f"有效字={vc}<{MIN_TEXT_LEN}；来源={classify_extraction_source(method)}；路径={method}"
                    )
                    if (
                        used_browser
                        and last_page_html
                        and page_indicates_404_or_missing(last_page_html)
                        and vc < MIN_TEXT_LEN
                    ):
                        stop_retry = True
                        last_err = (
                            f"页面为404/不存在类且有效字={vc}<{MIN_TEXT_LEN}，不再重试"
                        )
        except Exception as e:
            if _is_page_disconnected_error(e):
                row_disconnect_recoveries += 1
                if multitab is not None:
                    st, lk, c, nw, wi = multitab
                    restart_multitab_browser(st, lk, c, nw)
                    page = st[wi]
                else:
                    page = hard_restart_chromium_page(page, co)
                logger.warning(
                    "PageDisconnected，已重启 Chromium，本条不消耗重试次数立即再试 | id={} | Excel第{}行 | 累计恢复{}次",
                    cid,
                    idx + 2,
                    row_disconnect_recoveries,
                )
                if row_disconnect_recoveries > 40:
                    last_err = (
                        f"页面反复断连，已重启浏览器仍失败(>{40}次) | {type(e).__name__}"
                    )
                else:
                    attempt -= 1
                    time.sleep(min(2.0, max(0.3, float(args.attempt_wait_sec))))
                    continue
            else:
                last_err = f"{type(e).__name__}: {e}"[:900]
                logger.warning(
                    "抓取异常 id={} 第{}/{}次 Excel第{}行 | {}",
                    cid,
                    attempt,
                    max_retries,
                    idx + 2,
                    last_err,
                )

        if stop_retry:
            persist_crawl_failure(
                conn,
                cid,
                excel_meta_json,
                last_err,
                max_retries,
                max_retries,
                args.dry_run,
                db_lock=db_lock,
            )
            logger.warning(
                "id={} 已终止重试（404/失效页且正文过短）| Excel第{}行 | {}",
                cid,
                idx + 2,
                last_err,
            )
            st["fail"] = 1
            break

        persist_crawl_failure(
            conn,
            cid,
            excel_meta_json,
            last_err,
            attempt,
            max_retries,
            args.dry_run,
            db_lock=db_lock,
        )
        if attempt >= max_retries:
            logger.error(
                "id={} 已失败 {} 次，放弃。原因: {}",
                cid,
                max_retries,
                last_err,
            )
            st["fail"] = 1
            break
        logger.warning(
            "id={} 第{}次未达标: {} | {} 秒后同条再试…",
            cid,
            attempt,
            last_err,
            attempt_wait,
        )
        if attempt_wait > 0:
            time.sleep(attempt_wait)

    if succeeded:
        if not args.dry_run:
            with L:
                conn.execute(
                    sql_success,
                    (cid, main_html, time.time(), excel_meta_json),
                )
                if use_url_dedup and url_key:
                    conn.execute(
                        """INSERT INTO crawl_url_dedup(url_key, first_content_id, created_at)
                           VALUES (?,?,?)
                           ON CONFLICT(url_key) DO NOTHING""",
                        (url_key, cid, time.time()),
                    )
                conn.commit()
        vc = valid_text_cjk_digit_alpha(main_html)
        src = classify_extraction_source(method)
        if used_browser and got_json_first:
            ch = "JSON接口→不足→浏览器"
        elif used_browser:
            ch = "浏览器HTML"
        elif got_json_first:
            ch = "JSON接口"
        else:
            ch = "浏览器HTML"
        logger.success(
            "【已入库】结果库新写入 | id={} | 正文来源: {} | 抓取通道: {} | 技术路径: {} | "
            "有效字={} | {}/{} | {} | 源码{}字符",
            cid,
            src,
            ch,
            method,
            vc,
            cur_n,
            total_rows,
            url[:60] + ("…" if len(url) > 60 else ""),
            len(main_html or ""),
        )
        st["ok"] = 1
        if used_browser:
            st["ok_crawl"] = 1
        else:
            st["ok_json"] = 1

    time.sleep(args.interval)
    return page, st


def main():
    parser = argparse.ArgumentParser(description="Excel → 抓取正文 → SQLite")
    parser.add_argument("--excel", default=EXCEL_PATH)
    parser.add_argument("--db", default=SQLITE_DB)
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument(
        "--page-timeout",
        type=float,
        default=5,
        help="浏览器打开单条链接的最大等待秒数（超时仍取当前 DOM，默认 5，防一直转圈）",
    )
    parser.add_argument(
        "--browser-load-images",
        action="store_true",
        help="允许加载图片（默认关闭以提速）",
    )
    parser.add_argument(
        "--browser-load-mode",
        choices=("eager", "normal"),
        default="eager",
        help="eager=DOM 就绪即继续(快)；normal=等 load 事件（慢站可试）",
    )
    parser.add_argument("--wait", type=float, default=1.2)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--no-llm", action="store_true", help="不调用智谱，仅用规则")
    parser.add_argument(
        "--force-relearn-domain",
        action="store_true",
        help="忽略库里已缓存的域名 XPath，重新走大模型（仍按域名只问一次/次运行）",
    )
    parser.add_argument(
        "--url-col",
        default="",
        help="强制指定含详情链接的列名，多个用英文逗号分隔（表头与 Excel 完全一致）",
    )
    parser.add_argument(
        "--sheet",
        default="0",
        help="工作表：数字为从0开始的序号，否则为工作表名称",
    )
    parser.add_argument(
        "--header",
        type=int,
        default=0,
        help="表头在第几行（pandas 的 header=，默认0即第一行是列名）",
    )
    parser.add_argument(
        "--no-url-dedup",
        action="store_true",
        help="关闭 URL 去重（同一链接每次重新打开页面抓取）",
    )
    parser.add_argument(
        "--dup-url-skip",
        action="store_true",
        help="重复 URL 时不写入当前行（默认：复用已抓正文写入当前 id，不重复爬）",
    )
    parser.add_argument(
        "--no-json-fetch",
        action="store_true",
        help="不先 GET 解析 JSON（一律用浏览器抓 HTML）",
    )
    parser.add_argument(
        "--no-scroll-lazy",
        action="store_true",
        help="关闭「滚到底加载懒加载」直接取 HTML",
    )
    parser.add_argument(
        "--scroll-pause",
        type=float,
        default=0.55,
        help="每次滚底后的等待秒数（动态页可调大，如 0.8）",
    )
    parser.add_argument(
        "--scroll-max-rounds",
        type=int,
        default=40,
        help="最多滚底轮数（页面极长时可调大）",
    )
    parser.add_argument(
        "--sequential-excel",
        action="store_true",
        help="按 Excel 行顺序抓取（关闭按域名轮询，易连续刷同一站）",
    )
    parser.add_argument(
        "--schedule-seed",
        type=int,
        default=None,
        help="域名轮询随机种子（固定可复现顺序；不设则每次不同）",
    )
    parser.add_argument(
        "--max-crawl-retries",
        type=int,
        default=MAX_CRAWL_RETRIES_DEFAULT,
        help="单条任务失败后的最大尝试次数（默认20）",
    )
    parser.add_argument(
        "--attempt-wait-sec",
        type=int,
        default=ATTEMPT_WAIT_SEC_DEFAULT,
        help="同一条任务失败后、再次尝试前的等待秒数（默认8，与整表轮次无关）",
    )
    parser.add_argument(
        "--retry-wait-sec",
        type=int,
        default=ROUND_INTERVAL_SEC_DEFAULT,
        help="仅与 --loop 合用：每跑完一整轮 Excel 后休眠秒数再下一轮（默认600即10分钟）",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="永不退出：每轮处理完当前 Excel 任务后按 --retry-wait-sec 休眠，再重新读表开跑",
    )
    parser.add_argument(
        "--no-crawl-retry",
        action="store_true",
        help="失败不重试，只试一次",
    )
    parser.add_argument(
        "--force-recrawl",
        action="store_true",
        help="忽略「已成功」记录，强制重新抓取（默认达标则跳过）",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="并行标签数：单 Chromium，开 N 个标签同时加载不同链接；SQLite 加锁串行写入。建议 2～5",
    )
    args = parser.parse_args()
    if args.workers < 1:
        args.workers = 1

    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level:8}</level> | {message}",
        level="INFO",
        colorize=True,
    )

    if not os.path.isfile(args.excel):
        logger.error("找不到 Excel: {}", args.excel)
        sys.exit(1)

    init_sqlite(args.db)
    # 多标签 worker 线程会访问同一连接，必须关闭「仅创建线程可用」限制（配合 db_lock 串行）
    conn = sqlite3.connect(
        args.db, timeout=120.0, check_same_thread=False
    )
    migrate_cms_excel_meta_column(conn)
    zhipu_key = "" if args.no_llm else (ZHIPU_API_KEY or "").strip()
    if not zhipu_key and not args.no_llm:
        logger.warning(
            "ZHIPU_API_KEY 为空，大模型不可用（在 crawl_update_content.py 顶部填写或加 --no-llm）"
        )

    s_sheet = str(args.sheet).strip()
    try:
        sheet_key = int(s_sheet) if s_sheet.isdigit() else s_sheet
    except Exception:
        sheet_key = 0
    try:
        df = pd.read_excel(
            args.excel,
            engine="openpyxl",
            sheet_name=sheet_key,
            header=args.header,
        )
    except Exception as e:
        logger.exception("读取 Excel 失败: {} | sheet={} header={}", args.excel, sheet_key, args.header)
        sys.exit(1)

    df = normalize_columns(df)
    force_url = [x.strip() for x in args.url_col.split(",") if x.strip()]
    url_cols = detect_url_columns(df, force_url if force_url else None)
    if force_url and not url_cols:
        sys.exit(1)
    if not url_cols:
        logger.error(
            "未识别到含 http/https 链接的列。当前表头: {}\n"
            "请检查：1) 是否应用了 --header（表头若在第二行用 --header 1）\n"
            "       2) 或用 --url-col 你的列名 强制指定",
            list(df.columns),
        )
        sys.exit(1)

    logger.info(
        "Excel: {} | sheet={} | 数据行≈{} | 链接列(优先序): {}",
        os.path.basename(args.excel),
        sheet_key,
        len(df),
        url_cols,
    )
    logger.debug("全部表头: {}", list(df.columns))

    id_ok = False
    for c in df.columns:
        cl = str(c).strip().lower().replace(" ", "_")
        if cl in ("id", "aus_id", "ausid") or (
            "aus" in cl and "id" in cl and cl != "id"
        ):
            id_ok = True
            break
    if not id_ok:
        logger.warning(
            "表头中未匹配到 id / aus_id，若大量「无 id」请改列名或 --header。表头: {}",
            list(df.columns),
        )

    from DrissionPage import ChromiumPage

    pt = max(2.0, min(120.0, float(args.page_timeout)))

    end = len(df) if args.limit <= 0 else min(len(df), args.start + args.limit)
    rows = df.iloc[args.start:end]
    if len(rows) == 0:
        logger.warning("没有待处理行（检查 Excel 或 --start / --limit）")
        conn.close()
        return

    pre_fail = 0
    if args.sequential_excel:
        work_queue = list(rows.iterrows())
        total_rows = len(work_queue)
        dom_counts: dict[str, int] = {}
        logger.info("调度：按 Excel 顺序 | 有效任务 {} 条", total_rows)
    else:
        work_queue, pre_fail, dom_counts = build_domain_round_robin_queue(
            rows, url_cols, seed=args.schedule_seed
        )
        total_rows = len(work_queue)
        top = sorted(dom_counts.items(), key=lambda x: -x[1])[:12]
        logger.info(
            "调度：按域名轮询（每轮域名顺序随机）| 有效任务 {} 条 | 预处理跳过 {} 条（无id/无链接）| 域名 {} 个",
            total_rows,
            pre_fail,
            len(dom_counts),
        )
        logger.info("任务量前若干域名: {}", dict(top))
        if total_rows == 0:
            logger.error("无有效任务（全部行缺少 id 或链接）")
            conn.close()
            return

    domain_llm_asked: set[str] = set()
    domain_json_llm_asked: set[str] = set()
    domains_cache_cleared: set[str] = set()
    ok = ok_crawl = ok_json = ok_reuse = fail = skip_dup = skip_ok = 0
    fail += pre_fail
    use_url_dedup = not args.no_url_dedup

    logger.info(
        "开始处理 | workers={} | 本批任务={} | start={} | dry_run={} | db={} | URL去重={} | JSON优先={} | 滚底={} | 调度={}",
        args.workers,
        total_rows,
        args.start,
        args.dry_run,
        args.db,
        use_url_dedup,
        not args.no_json_fetch,
        not args.no_scroll_lazy,
        "Excel顺序" if args.sequential_excel else "域名轮询",
    )
    logger.info(
        "间隔策略 | 同一条任务失败再试: {}s | 整表轮次间隔: {}（{}）",
        args.attempt_wait_sec,
        f"{args.retry_wait_sec}s" if args.loop else "—",
        "每轮结束休眠后重读Excel" if args.loop else "单轮结束即退出，加 --loop 才用轮次间隔",
    )

    sql_success = """INSERT INTO cms_crawl_data_content(
          id, description, updated_at, excel_meta, crawl_status, crawl_error, crawl_fail_count)
        VALUES (?,?,?,?, 'ok', NULL, 0)
        ON CONFLICT(id) DO UPDATE SET
          description=excluded.description,
          updated_at=excluded.updated_at,
          excel_meta=excluded.excel_meta,
          crawl_status='ok',
          crawl_error=NULL,
          crawl_fail_count=0"""

    db_lock = threading.Lock() if args.workers > 1 else None
    domain_lock = threading.Lock() if args.workers > 1 else None
    page = None
    co = None
    if args.workers == 1:
        co = build_chromium_options(args)
        page = ChromiumPage(addr_or_opts=co)
        apply_auto_accept_browser_dialogs(page)
        logger.info(
            "浏览器优化 | 单页超时={}s | 图片={} | 加载模式={} | 静音=开 | JS原生弹窗=自动确定 | workers=1",
            pt,
            "开" if args.browser_load_images else "关(提速)",
            args.browser_load_mode,
        )
    round_no = 0
    while True:
        round_no += 1
        if round_no > 1:
            if args.dry_run:
                break
            logger.info(
                "======== 第 {} 轮：整表轮次间隔 {} 秒后开始（重读 Excel）========",
                round_no,
                args.retry_wait_sec,
            )
            time.sleep(max(0, int(args.retry_wait_sec)))
            try:
                df = pd.read_excel(
                    args.excel,
                    engine="openpyxl",
                    sheet_name=sheet_key,
                    header=args.header,
                )
            except Exception as e:
                logger.exception("第 {} 轮读取 Excel 失败: {} | 稍后按轮次间隔重试", round_no, e)
                continue
            df = normalize_columns(df)
            url_cols_new = detect_url_columns(df, force_url if force_url else None)
            if force_url and not url_cols_new:
                logger.error("第 {} 轮：强制链接列无效，终止", round_no)
                break
            if not url_cols_new:
                logger.warning("第 {} 轮：未识别链接列，跳过本轮", round_no)
                continue
            url_cols.clear()
            url_cols.extend(url_cols_new)
            end = len(df) if args.limit <= 0 else min(len(df), args.start + args.limit)
            rows = df.iloc[args.start:end]
            if len(rows) == 0:
                logger.warning("第 {} 轮：无数据行", round_no)
                continue
            if args.sequential_excel:
                work_queue = list(rows.iterrows())
                total_rows = len(work_queue)
            else:
                work_queue, pre_fail_r, dom_counts = build_domain_round_robin_queue(
                    rows, url_cols, seed=args.schedule_seed
                )
                total_rows = len(work_queue)
                fail += pre_fail_r
            if total_rows == 0:
                logger.warning("第 {} 轮：无有效任务", round_no)
                continue
            logger.info("第 {} 轮：有效任务 {} 条", round_no, total_rows)

        logger.info(
            "======== 第 {} 轮执行中 | 本批 {} 条 ========",
            round_no,
            total_rows,
        )
        if args.workers > 1:
            mco = build_chromium_options(args)
            mroot = ChromiumPage(addr_or_opts=mco)
            apply_auto_accept_browser_dialogs(mroot)
            shared_tabs: list = [mroot]
            for _ in range(args.workers - 1):
                shared_tabs.append(
                    mroot.new_tab(url="about:blank", background=True)
                )
            browser_tab_lock = threading.Lock()
            mt_bundle = (
                shared_tabs,
                browser_tab_lock,
                mco,
                args.workers,
            )
            logger.info(
                "单浏览器多标签 | 标签数={} | 单页超时={}s | 图片={} | SQLite=串行写入 | 调度={}",
                args.workers,
                pt,
                "开" if args.browser_load_images else "关(提速)",
                "Excel顺序" if args.sequential_excel else "域名轮询",
            )

            task_q: queue.Queue = queue.Queue()
            for cur_n, (idx, row) in enumerate(work_queue, start=1):
                task_q.put((idx, row, cur_n))
            for _ in range(args.workers):
                task_q.put(None)

            agg: dict[str, int] = defaultdict(int)
            agg_lock = threading.Lock()

            def worker_fn(wid: int) -> None:
                multitab = (*mt_bundle, wid)
                wtab = shared_tabs[wid]
                while True:
                    item = task_q.get()
                    if item is None:
                        task_q.task_done()
                        break
                    idx, row, cur_n = item
                    try:
                        wtab, delta = crawl_process_one_row(
                            idx,
                            row,
                            cur_n,
                            total_rows,
                            url_cols,
                            args,
                            conn,
                            sql_success,
                            wtab,
                            mco,
                            pt,
                            use_url_dedup,
                            zhipu_key,
                            domain_llm_asked,
                            domain_json_llm_asked,
                            domains_cache_cleared,
                            db_lock,
                            domain_lock,
                            multitab=multitab,
                        )
                        with agg_lock:
                            for k, v in delta.items():
                                agg[k] = agg.get(k, 0) + v
                    except Exception as row_exc:
                        err_msg = f"{type(row_exc).__name__}: {row_exc}"[:900]
                        logger.exception(
                            "并行任务异常 | Excel第{}行 | {}",
                            idx + 2,
                            err_msg,
                        )
                        with agg_lock:
                            agg["fail"] = agg.get("fail", 0) + 1
                        try:
                            cid2 = resolve_content_id(row)
                            if cid2 is not None and not args.dry_run:
                                try:
                                    ej = pandas_row_to_excel_meta(row)
                                except Exception:
                                    ej = "{}"
                                try:
                                    persist_crawl_failure(
                                        conn,
                                        cid2,
                                        ej,
                                        f"未捕获异常:{err_msg}",
                                        1,
                                        1,
                                        args.dry_run,
                                        db_lock=db_lock,
                                    )
                                except Exception as pe:
                                    logger.warning("写入本条失败状态异常（忽略）| {}", pe)
                        except Exception:
                            pass
                    finally:
                        task_q.task_done()

            threads = [
                threading.Thread(
                    target=worker_fn, args=(w,), name=f"crawl-tab{w}", daemon=False
                )
                for w in range(args.workers)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            try:
                shared_tabs[0].quit()
            except Exception:
                pass
            ok += agg.get("ok", 0)
            fail += agg.get("fail", 0)
            ok_crawl += agg.get("ok_crawl", 0)
            ok_json += agg.get("ok_json", 0)
            ok_reuse += agg.get("ok_reuse", 0)
            skip_dup += agg.get("skip_dup", 0)
            skip_ok += agg.get("skip_ok", 0)
        else:
            for cur_n, (idx, row) in enumerate(work_queue, start=1):
                try:
                    page, delta = crawl_process_one_row(
                        idx,
                        row,
                        cur_n,
                        total_rows,
                        url_cols,
                        args,
                        conn,
                        sql_success,
                        page,
                        co,
                        pt,
                        use_url_dedup,
                        zhipu_key,
                        domain_llm_asked,
                        domain_json_llm_asked,
                        domains_cache_cleared,
                        None,
                        None,
                    )
                    ok += delta["ok"]
                    fail += delta["fail"]
                    ok_crawl += delta["ok_crawl"]
                    ok_json += delta["ok_json"]
                    ok_reuse += delta["ok_reuse"]
                    skip_dup += delta["skip_dup"]
                    skip_ok += delta["skip_ok"]
                except Exception as row_exc:
                    err_msg = f"{type(row_exc).__name__}: {row_exc}"[:900]
                    logger.exception(
                        "本条任务异常已跳过，进程继续 | Excel第{}行 | {}",
                        idx + 2,
                        err_msg,
                    )
                    fail += 1
                    try:
                        cid2 = resolve_content_id(row)
                        if cid2 is not None and not args.dry_run:
                            try:
                                ej = pandas_row_to_excel_meta(row)
                            except Exception:
                                ej = "{}"
                            try:
                                persist_crawl_failure(
                                    conn,
                                    cid2,
                                    ej,
                                    f"未捕获异常:{err_msg}",
                                    1,
                                    1,
                                    args.dry_run,
                                )
                            except Exception as pe:
                                logger.warning("写入本条失败状态异常（忽略）| {}", pe)
                    except Exception:
                        pass
                    if page is not None and co is not None:
                        page = reconnect_chromium_if_needed(page, co, row_exc)
                    time.sleep(args.interval)

        logger.info("======== 第 {} 轮本批已跑完 ========", round_no)
        if not args.loop or args.dry_run:
            if args.loop and args.dry_run:
                logger.info("--dry-run 与 --loop 同用时只跑一轮")
            break

    if page is not None:
        try:
            page.quit()
        except Exception:
            pass
    conn.close()
    logger.info(
        "全部结束 | 成功={}（含: 结果库已存在跳过={} | 本次浏览器抓取={} | 本次仅JSON接口={} | 同URL复用库内正文={}）"
        "| 失败={} | 重复URL策略跳过={} | {}",
        ok,
        skip_ok,
        ok_crawl,
        ok_json,
        ok_reuse,
        fail,
        skip_dup,
        args.db,
    )


if __name__ == "__main__":
    main()
