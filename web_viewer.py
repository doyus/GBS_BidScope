# -*- coding: utf-8 -*-
"""
浏览入库正文 + Excel 元数据 + 数据分析平台。
运行: python web_viewer.py  →  http://127.0.0.1:5050
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import io
from datetime import datetime, timedelta

from flask import (
    Flask,
    Response,
    abort,
    flash,
    redirect,
    render_template,
    request,
    url_for,
    send_file,
)

import pandas as pd
from data_analytics import (
    get_full_analytics,
    get_data_for_export,
    parse_meta,
)

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "local-viewer-dev-key")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(BASE_DIR, "crawl_local.db")

# 列表优先展示的 Excel 列（有则显示，无则跳过；其余在详情页看全）
LIST_META_PRIORITY = (
    "标题",
    "主域名",
    "来源",
    "aus_id",
    "详情页地址_链接",
    "Comment地址",
    "详情页",
    "Content地址",
    "源域名",
)


def db_path() -> str:
    return os.environ.get("SQLITE_DB", DEFAULT_DB)


def get_conn():
    p = db_path()
    if not os.path.isfile(p):
        raise FileNotFoundError(f"数据库不存在: {p}")
    conn = sqlite3.connect(p)
    cur = conn.execute("PRAGMA table_info(cms_crawl_data_content)")
    have = {r[1] for r in cur.fetchall()}
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
            have.add(col)
            conn.commit()
    return conn


def parse_meta(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        d = json.loads(raw)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def strip_tags_preview(s: str, max_len: int = 100) -> str:
    if not s:
        return ""
    t = re.sub(r"<[^>]+>", " ", s)
    t = re.sub(r"\s+", " ", t).strip()
    return t[:max_len] + ("…" if len(t) > max_len else "")


def body_text_stats(html: str) -> dict:
    """去标签后统计：中文(CJK)、数字、字母；不计空格标点。"""
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


def short_url(s: str, n: int = 42) -> str:
    if not s:
        return "—"
    s = str(s).strip()
    return s[:n] + ("…" if len(s) > n else "")


def list_columns_from_meta(sample_metas: list[dict]) -> list[str]:
    """本页出现过的列名，按 LIST_META_PRIORITY 再补全其余键。"""
    all_keys: set[str] = set()
    for m in sample_metas:
        all_keys.update(m.keys())
    ordered = [k for k in LIST_META_PRIORITY if k in all_keys]
    rest = sorted(all_keys - set(ordered))
    return ordered + rest


# 列表统一按「最后更新时间」从新到旧；兼容秒/毫秒时间戳，无时间排后
SQL_ORDER_BY_LATEST = (
    "(CASE WHEN updated_at IS NULL OR TRIM(CAST(updated_at AS TEXT)) IN ('','0') THEN 0.0 "
    "WHEN ABS(CAST(updated_at AS REAL)) > 1e11 THEN CAST(updated_at AS REAL) / 1000.0 "
    "ELSE CAST(updated_at AS REAL) END) DESC, id DESC"
)


def _status_where(status: str) -> tuple[str, tuple]:
    """列表筛选：all | ok | failed | retrying | problem"""
    s = (status or "all").strip().lower()
    if s == "ok":
        return (
            " (IFNULL(crawl_status,'') IN ('ok','') AND (IFNULL(crawl_error,'')='' OR crawl_status='ok') "
            "AND (LENGTH(IFNULL(description,''))>80 OR crawl_status='ok')) ",
            (),
        )
    if s == "failed":
        return (" IFNULL(crawl_status,'') = 'failed' ", ())
    if s == "retrying":
        return (" IFNULL(crawl_status,'') = 'retrying' ", ())
    if s == "problem":
        return (
            " (IFNULL(crawl_status,'') IN ('failed','retrying') "
            "OR (IFNULL(crawl_error,'')!='' AND IFNULL(crawl_status,'')!='ok')) ",
            (),
        )
    return (" 1=1 ", ())


@app.route("/")
def index():
    page = max(1, int(request.args.get("page", 1)))
    q = (request.args.get("q") or "").strip()
    status_filter = (request.args.get("status") or "all").strip()
    per_page = min(100, max(10, int(request.args.get("per", 30))))

    try:
        conn = get_conn()
    except FileNotFoundError as e:
        return render_template("viewer_error.html", error=str(e)), 503

    sw, _ = _status_where(status_filter)
    off = (page - 1) * per_page
    cur = conn.cursor()
    if q:
        if q.isdigit():
            cur.execute(
                f"SELECT COUNT(*) FROM cms_crawl_data_content WHERE id = ? AND ({sw})",
                (int(q),),
            )
            total = cur.fetchone()[0]
            cur.execute(
                f"""SELECT id, description, updated_at, excel_meta,
                   IFNULL(crawl_status,'') AS cs, IFNULL(crawl_error,'') AS ce,
                   IFNULL(crawl_fail_count,0) AS cf
                   FROM cms_crawl_data_content WHERE id = ? AND ({sw})
                   ORDER BY {SQL_ORDER_BY_LATEST} LIMIT ? OFFSET ?""",
                (int(q), per_page, off),
            )
        else:
            like = f"%{q}%"
            cur.execute(
                f"""SELECT COUNT(*) FROM cms_crawl_data_content
                   WHERE ({sw}) AND (
                   description LIKE ? OR IFNULL(excel_meta,'') LIKE ?
                   OR IFNULL(crawl_error,'') LIKE ?)""",
                (like, like, like),
            )
            total = cur.fetchone()[0]
            cur.execute(
                f"""SELECT id, description, updated_at, excel_meta,
                   IFNULL(crawl_status,'') AS cs, IFNULL(crawl_error,'') AS ce,
                   IFNULL(crawl_fail_count,0) AS cf
                   FROM cms_crawl_data_content
                   WHERE ({sw}) AND (
                   description LIKE ? OR IFNULL(excel_meta,'') LIKE ?
                   OR IFNULL(crawl_error,'') LIKE ?)
                   ORDER BY {SQL_ORDER_BY_LATEST} LIMIT ? OFFSET ?""",
                (like, like, like, per_page, off),
            )
    else:
        cur.execute(f"SELECT COUNT(*) FROM cms_crawl_data_content WHERE ({sw})")
        total = cur.fetchone()[0]
        cur.execute(
            f"""SELECT id, description, updated_at, excel_meta,
               IFNULL(crawl_status,'') AS cs, IFNULL(crawl_error,'') AS ce,
               IFNULL(crawl_fail_count,0) AS cf
               FROM cms_crawl_data_content
               WHERE ({sw})
               ORDER BY {SQL_ORDER_BY_LATEST} LIMIT ? OFFSET ?""",
            (per_page, off),
        )

    raw_rows = cur.fetchall()
    conn.close()

    metas = [parse_meta(r[3]) for r in raw_rows]
    # 列表只展示部分列，避免过宽
    priority_keys = [k for k in LIST_META_PRIORITY]
    extra_from_page = list_columns_from_meta(metas)
    display_keys = []
    for k in priority_keys:
        if k in extra_from_page and k not in display_keys:
            display_keys.append(k)
    for k in extra_from_page:
        if k not in display_keys and len(display_keys) < 8:
            display_keys.append(k)

    rows = []
    for raw, meta in zip(raw_rows, metas):
        rid, desc, ts, _em = raw[0], raw[1], raw[2], raw[3]
        cstat, cerr, cfc = (raw[4] or ""), (raw[5] or ""), raw[6]
        desc = desc or ""
        cells = {}
        for k in display_keys:
            v = meta.get(k)
            if v is None:
                cells[k] = "—"
            else:
                sv = str(v).strip()
                if any(
                    x in k
                    for x in ("链接", "地址", "URL", "url", "http", "Content", "详情")
                ):
                    cells[k] = short_url(sv, 36)
                else:
                    cells[k] = short_url(sv, 48) if len(sv) > 48 else sv or "—"
        st = body_text_stats(desc)
        err_short = (cerr[:56] + "…") if len(cerr) > 56 else cerr
        st_raw = (cstat or "").strip().lower()
        if st_raw == "failed":
            row_cls = "row-failed"
        elif st_raw == "retrying":
            row_cls = "row-retrying"
        else:
            row_cls = ""
        rows.append(
            {
                "id": rid,
                "cells": cells,
                "preview": strip_tags_preview(desc),
                "text_total": st["total"],
                "text_cn": st["cn"],
                "text_digit": st["digit"],
                "text_alpha": st["alpha"],
                "updated": _fmt_ts(ts),
                "meta_extra_count": max(0, len(meta) - len(display_keys)),
                "crawl_status": cstat or "—",
                "crawl_error_short": err_short or "—",
                "crawl_error_full": cerr,
                "crawl_fail_count": cfc,
                "status_class": row_cls,
            }
        )

    pages = max(1, (total + per_page - 1) // per_page)
    return render_template(
        "viewer_index.html",
        rows=rows,
        display_keys=display_keys,
        page=page,
        pages=pages,
        total=total,
        per_page=per_page,
        q=q,
        db_file=db_path(),
        status_filter=status_filter,
    )


def _fmt_ts(ts) -> str:
    if ts is None:
        return "—"
    try:
        if ts > 1e12:
            ts = ts / 1000.0
        return datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


@app.route("/item/<int:rid>")
def item(rid: int):
    try:
        conn = get_conn()
    except FileNotFoundError as e:
        return render_template("viewer_error.html", error=str(e)), 503
    cur = conn.execute(
        """SELECT id, description, updated_at, excel_meta,
           IFNULL(crawl_status,''), IFNULL(crawl_error,''), IFNULL(crawl_fail_count,0)
           FROM cms_crawl_data_content WHERE id = ?""",
        (rid,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        abort(404)
    _id, desc, ts, meta_raw = row[0], row[1], row[2], row[3]
    crawl_status, crawl_error, crawl_fail_count = row[4], row[5], row[6]
    desc = desc or ""
    meta = parse_meta(meta_raw)
    meta_items = sorted(meta.items(), key=lambda x: x[0])
    st = body_text_stats(desc)
    return render_template(
        "viewer_detail.html",
        rid=_id,
        html_len=len(desc),
        text_stats=st,
        updated=_fmt_ts(ts),
        render_url=url_for("render_html", rid=rid),
        raw_url=url_for("api_raw_html", rid=rid),
        meta_items=meta_items,
        crawl_status=crawl_status or "—",
        crawl_error=crawl_error or "",
        crawl_fail_count=crawl_fail_count or 0,
    )


@app.route("/api/raw/<int:rid>")
def api_raw_html(rid: int):
    try:
        conn = get_conn()
    except FileNotFoundError:
        abort(503)
    cur = conn.execute(
        "SELECT description FROM cms_crawl_data_content WHERE id = ?", (rid,)
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        abort(404)
    html = row[0] or ""
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    return Response(html, mimetype="text/plain; charset=utf-8")


@app.route("/render/<int:rid>")
def render_html(rid: int):
    try:
        conn = get_conn()
    except FileNotFoundError:
        abort(503)
    cur = conn.execute(
        "SELECT description FROM cms_crawl_data_content WHERE id = ?", (rid,)
    )
    row = cur.fetchone()
    conn.close()
    if not row or not row[0]:
        abort(404)
    html = row[0]
    if isinstance(html, bytes):
        html = html.decode("utf-8", errors="replace")
    return Response(
        html,
        mimetype="text/html; charset=utf-8",
        headers={"X-Frame-Options": "SAMEORIGIN"},
    )


# 清空时依次删除的表（爬虫相关，不可恢复）
_TABLES_TO_CLEAR = (
    "cms_crawl_data_content",
    "crawl_url_dedup",
    "domain_learned_xpath",
    "domain_json_html_path",
)


def _table_counts(conn: sqlite3.Connection) -> dict[str, int]:
    out: dict[str, int] = {}
    for t in _TABLES_TO_CLEAR:
        try:
            out[t] = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        except sqlite3.OperationalError:
            out[t] = -1
    return out


def _clear_crawl_tables(conn: sqlite3.Connection) -> dict[str, int]:
    """返回各表删除前条数。"""
    before = {}
    for t in _TABLES_TO_CLEAR:
        try:
            n = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            conn.execute(f"DELETE FROM {t}")
            before[t] = n
        except sqlite3.OperationalError:
            before[t] = -1
    conn.commit()
    return before


@app.route("/admin/clear", methods=["GET", "POST"])
def admin_clear():
    confirm_word = "清空"
    try:
        conn = get_conn()
    except FileNotFoundError as e:
        return render_template("viewer_error.html", error=str(e)), 503

    if request.method == "POST":
        if (request.form.get("confirm") or "").strip() != confirm_word:
            conn.close()
            flash("验证失败：请在输入框中准确输入「清空」二字", "error")
            return redirect(url_for("admin_clear"))
        counts = _clear_crawl_tables(conn)
        conn.close()
        main_n = counts.get("cms_crawl_data_content", 0)
        flash(
            f"已清空：正文 {main_n} 条；URL 去重 / XPath / JSON 路径缓存已一并删除。",
            "success",
        )
        return redirect(url_for("index"))

    counts = _table_counts(conn)
    conn.close()
    return render_template(
        "viewer_admin_clear.html",
        counts=counts,
        db_file=db_path(),
        confirm_word=confirm_word,
    )


@app.route("/health")
def health():
    return {"ok": True, "db": os.path.isfile(db_path())}


# ==================== 数据分析平台功能 ====================

@app.route("/analytics")
def analytics_dashboard():
    """数据分析仪表盘"""
    try:
        report = get_full_analytics(db_path())
    except Exception as e:
        return render_template("viewer_error.html", error=str(e)), 500
    
    return render_template("viewer_analytics.html", report=report)


@app.route("/api/analytics/data")
def api_analytics_data():
    """API: 获取分析数据（用于图表）"""
    try:
        report = get_full_analytics(db_path())
        return report
    except Exception as e:
        return {"error": str(e)}, 500


@app.route("/api/analytics/daily-trend")
def api_daily_trend():
    """API: 每日抓取趋势数据"""
    try:
        conn = get_conn()
        df = pd.read_sql_query("""
            SELECT updated_at, crawl_status 
            FROM cms_crawl_data_content
            WHERE updated_at IS NOT NULL
        """, conn)
        conn.close()
        
        def parse_ts(ts):
            try:
                ts_val = float(ts)
                if ts_val > 1e12:
                    ts_val = ts_val / 1000.0
                return datetime.fromtimestamp(ts_val).strftime("%Y-%m-%d")
            except:
                return None
        
        df["date"] = df["updated_at"].apply(parse_ts)
        df = df[df["date"].notna()]
        
        daily = df.groupby(["date", "crawl_status"]).size().unstack(fill_value=0)
        daily["total"] = daily.sum(axis=1)
        
        result = {
            "dates": daily.index.tolist(),
            "total": daily.get("total", [0] * len(daily)).tolist(),
            "success": daily.get("ok", [0] * len(daily)).tolist(),
            "failed": daily.get("failed", [0] * len(daily)).tolist(),
        }
        return result
    except Exception as e:
        return {"error": str(e)}, 500


@app.route("/api/analytics/domain-stats")
def api_domain_stats():
    """API: 域名统计数据"""
    try:
        conn = get_conn()
        df = pd.read_sql_query("""
            SELECT excel_meta, crawl_status 
            FROM cms_crawl_data_content
        """, conn)
        conn.close()
        
        df["domain"] = df["excel_meta"].apply(
            lambda x: parse_meta(x).get("主域名", "未知")
        )
        
        domain_stats = df.groupby(["domain", "crawl_status"]).size().unstack(fill_value=0)
        domain_stats["total"] = domain_stats.sum(axis=1)
        domain_stats = domain_stats.sort_values("total", ascending=False).head(15)
        
        result = {
            "domains": domain_stats.index.tolist(),
            "total": domain_stats.get("total", [0] * len(domain_stats)).tolist(),
            "success": domain_stats.get("ok", [0] * len(domain_stats)).tolist(),
            "failed": domain_stats.get("failed", [0] * len(domain_stats)).tolist(),
        }
        return result
    except Exception as e:
        return {"error": str(e)}, 500


@app.route("/api/analytics/content-length")
def api_content_length():
    """API: 内容长度分布"""
    try:
        conn = get_conn()
        df = pd.read_sql_query("""
            SELECT description 
            FROM cms_crawl_data_content
            WHERE description IS NOT NULL
        """, conn)
        conn.close()
        
        def calc_length(html):
            if not html:
                return 0
            plain = re.sub(r"<[^>]+>", " ", html)
            plain = re.sub(r"\s+", " ", plain)
            return len(re.findall(r"[\u4e00-\u9fff]", plain)) + len(re.findall(r"\d", plain)) + len(re.findall(r"[A-Za-z]", plain))
        
        df["length"] = df["description"].apply(calc_length)
        
        bins = [0, 100, 500, 1000, 5000, float('inf')]
        labels = ["0-100", "100-500", "500-1000", "1000-5000", "5000+"]
        df["range"] = pd.cut(df["length"], bins=bins, labels=labels, right=False)
        
        dist = df["range"].value_counts().sort_index()
        
        return {
            "ranges": labels,
            "counts": [int(dist.get(r, 0)) for r in labels]
        }
    except Exception as e:
        return {"error": str(e)}, 500


@app.route("/export")
def export_page():
    """数据导出页面"""
    try:
        conn = get_conn()
        
        # 获取所有域名
        domains = set()
        cur = conn.execute("SELECT excel_meta FROM cms_crawl_data_content WHERE excel_meta IS NOT NULL")
        for row in cur.fetchall():
            meta = parse_meta(row[0])
            domain = meta.get("主域名", "未知")
            domains.add(domain)
        conn.close()
        
        return render_template("viewer_export.html", domains=sorted(domains))
    except Exception as e:
        return render_template("viewer_error.html", error=str(e)), 500


@app.route("/export/excel")
def export_excel():
    """导出 Excel"""
    try:
        status = request.args.get("status", "all")
        domain = request.args.get("domain", "all")
        date_from = request.args.get("date_from")
        date_to = request.args.get("date_to")
        
        df = get_data_for_export(db_path(), status, domain if domain != "all" else None, date_from, date_to)
        
        if df.empty:
            flash("没有符合条件的数据", "error")
            return redirect(url_for("export_page"))
        
        # 准备导出数据
        export_data = []
        for _, row in df.iterrows():
            meta = row.get("meta_dict", {})
            export_data.append({
                "ID": row["id"],
                "标题": meta.get("标题", ""),
                "主域名": meta.get("主域名", ""),
                "来源": meta.get("来源", ""),
                "抓取状态": row["crawl_status"] or "ok",
                "失败次数": row["crawl_fail_count"],
                "错误信息": row["crawl_error"] or "",
                "更新时间": row["parsed_time"].strftime("%Y-%m-%d %H:%M:%S") if row["parsed_time"] else "",
            })
        
        export_df = pd.DataFrame(export_data)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            export_df.to_excel(writer, index=False, sheet_name="招投标数据")
        output.seek(0)
        
        return send_file(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"招投标数据_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
    except Exception as e:
        flash(f"导出失败: {str(e)}", "error")
        return redirect(url_for("export_page"))


@app.route("/export/csv")
def export_csv():
    """导出 CSV"""
    try:
        status = request.args.get("status", "all")
        domain = request.args.get("domain", "all")
        date_from = request.args.get("date_from")
        date_to = request.args.get("date_to")
        
        df = get_data_for_export(db_path(), status, domain if domain != "all" else None, date_from, date_to)
        
        if df.empty:
            flash("没有符合条件的数据", "error")
            return redirect(url_for("export_page"))
        
        # 准备导出数据
        export_data = []
        for _, row in df.iterrows():
            meta = row.get("meta_dict", {})
            export_data.append({
                "ID": row["id"],
                "标题": meta.get("标题", ""),
                "主域名": meta.get("主域名", ""),
                "来源": meta.get("来源", ""),
                "抓取状态": row["crawl_status"] or "ok",
                "失败次数": row["crawl_fail_count"],
                "错误信息": row["crawl_error"] or "",
                "更新时间": row["parsed_time"].strftime("%Y-%m-%d %H:%M:%S") if row["parsed_time"] else "",
            })
        
        export_df = pd.DataFrame(export_data)
        
        output = io.StringIO()
        export_df.to_csv(output, index=False, encoding="utf-8-sig")
        output.seek(0)
        
        return Response(
            output.getvalue(),
            mimetype="text/csv; charset=utf-8-sig",
            headers={
                "Content-Disposition": f"attachment; filename=招投标数据_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            }
        )
    except Exception as e:
        flash(f"导出失败: {str(e)}", "error")
        return redirect(url_for("export_page"))


@app.route("/export/report")
def export_report():
    """导出分析报告（HTML格式，可打印为PDF）"""
    try:
        report = get_full_analytics(db_path())
        
        html_content = render_template("viewer_report.html", report=report, generated_at=datetime.now())
        
        return Response(
            html_content,
            mimetype="text/html; charset=utf-8",
            headers={
                "Content-Disposition": f"attachment; filename=数据分析报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
            }
        )
    except Exception as e:
        flash(f"报告生成失败: {str(e)}", "error")
        return redirect(url_for("analytics_dashboard"))


if __name__ == "__main__":
    print("数据库:", db_path())
    print("打开 http://127.0.0.1:5050")
    app.run(host="127.0.0.1", port=int(os.environ.get("PORT", "5050")), debug=True)
