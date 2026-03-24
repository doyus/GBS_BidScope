# -*- coding: utf-8 -*-
"""
浏览入库正文 + Excel 元数据 + 数据分析平台
运行: python web_viewer.py → http://127.0.0.1:5050
"""
from __future__ import annotations

import io
import os
from datetime import datetime
from typing import Any, Optional

from flask import (
    Flask,
    Response,
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
import pandas as pd

from config import config
from database import get_connection, get_db_path, clear_all_tables, get_table_counts
from utils import (
    parse_meta,
    strip_tags_preview,
    count_text_stats,
    shorten_url,
    format_timestamp,
    build_status_where,
    get_order_by_sql,
)
from data_analytics import get_full_analytics, get_data_for_export

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False
app.secret_key = config.flask.secret_key


def _get_list_display_keys(metas: list[dict[str, Any]]) -> list[str]:
    """获取列表页显示的键"""
    all_keys: set[str] = set()
    for meta in metas:
        all_keys.update(meta.keys())

    ordered = [k for k in config.display.list_meta_priority if k in all_keys]
    rest = sorted(all_keys - set(ordered))
    return ordered + rest


def _build_row_data(
    raw: tuple, meta: dict[str, Any], display_keys: list[str]
) -> dict[str, Any]:
    """构建行数据"""
    rid, desc, ts, _em = raw[0], raw[1], raw[2], raw[3]
    crawl_status = raw[4] or ""
    crawl_error = raw[5] or ""
    crawl_fail_count = raw[6]

    desc = desc or ""
    cells = _build_cells(meta, display_keys)
    stats = count_text_stats(desc)
    error_short = (crawl_error[:56] + "…") if len(crawl_error) > 56 else crawl_error

    status_class = _get_status_class(crawl_status)

    return {
        "id": rid,
        "cells": cells,
        "preview": strip_tags_preview(desc),
        "text_total": stats["total"],
        "text_cn": stats["cn"],
        "text_digit": stats["digit"],
        "text_alpha": stats["alpha"],
        "updated": format_timestamp(ts),
        "meta_extra_count": max(0, len(meta) - len(display_keys)),
        "crawl_status": crawl_status or "—",
        "crawl_error_short": error_short or "—",
        "crawl_error_full": crawl_error,
        "crawl_fail_count": crawl_fail_count,
        "status_class": status_class,
    }


def _build_cells(meta: dict[str, Any], display_keys: list[str]) -> dict[str, str]:
    """构建单元格数据"""
    cells: dict[str, str] = {}
    for key in display_keys:
        value = meta.get(key)
        if value is None:
            cells[key] = "—"
            continue

        str_val = str(value).strip()
        url_fields = ("链接", "地址", "URL", "url", "http", "Content", "详情")
        max_len = 36 if any(x in key for x in url_fields) else 48
        cells[key] = shorten_url(str_val, max_len)

    return cells


def _get_status_class(crawl_status: str) -> str:
    """获取状态CSS类"""
    status = crawl_status.strip().lower()
    if status == "failed":
        return "row-failed"
    if status == "retrying":
        return "row-retrying"
    return ""


def _execute_index_query(
    conn: Any, query: str, status_filter: str, search: str, per_page: int, offset: int
) -> tuple[int, list]:
    """执行首页查询"""
    cur = conn.cursor()
    where_clause, _ = build_status_where(status_filter)

    if search:
        if search.isdigit():
            return _query_by_id(cur, where_clause, int(search), per_page, offset)
        return _query_by_search(cur, where_clause, search, per_page, offset)

    return _query_all(cur, where_clause, per_page, offset)


def _query_by_id(
    cur: Any, where_clause: str, content_id: int, per_page: int, offset: int
) -> tuple[int, list]:
    """按ID查询"""
    cur.execute(
        f"SELECT COUNT(*) FROM {config.database.table_content} "
        f"WHERE id = ? AND ({where_clause})",
        (content_id,),
    )
    total = cur.fetchone()[0]

    cur.execute(
        f"""SELECT id, description, updated_at, excel_meta,
           IFNULL(crawl_status,'') AS cs, IFNULL(crawl_error,'') AS ce,
           IFNULL(crawl_fail_count,0) AS cf
           FROM {config.database.table_content}
           WHERE id = ? AND ({where_clause})
           ORDER BY {get_order_by_sql()} LIMIT ? OFFSET ?""",
        (content_id, per_page, offset),
    )
    return total, cur.fetchall()


def _query_by_search(
    cur: Any, where_clause: str, search: str, per_page: int, offset: int
) -> tuple[int, list]:
    """按关键词搜索"""
    like = f"%{search}%"

    cur.execute(
        f"""SELECT COUNT(*) FROM {config.database.table_content}
           WHERE ({where_clause}) AND (
           description LIKE ? OR IFNULL(excel_meta,'') LIKE ?
           OR IFNULL(crawl_error,'') LIKE ?)""",
        (like, like, like),
    )
    total = cur.fetchone()[0]

    cur.execute(
        f"""SELECT id, description, updated_at, excel_meta,
           IFNULL(crawl_status,'') AS cs, IFNULL(crawl_error,'') AS ce,
           IFNULL(crawl_fail_count,0) AS cf
           FROM {config.database.table_content}
           WHERE ({where_clause}) AND (
           description LIKE ? OR IFNULL(excel_meta,'') LIKE ?
           OR IFNULL(crawl_error,'') LIKE ?)
           ORDER BY {get_order_by_sql()} LIMIT ? OFFSET ?""",
        (like, like, like, per_page, offset),
    )
    return total, cur.fetchall()


def _query_all(
    cur: Any, where_clause: str, per_page: int, offset: int
) -> tuple[int, list]:
    """查询全部"""
    cur.execute(
        f"SELECT COUNT(*) FROM {config.database.table_content} WHERE ({where_clause})"
    )
    total = cur.fetchone()[0]

    cur.execute(
        f"""SELECT id, description, updated_at, excel_meta,
           IFNULL(crawl_status,'') AS cs, IFNULL(crawl_error,'') AS ce,
           IFNULL(crawl_fail_count,0) AS cf
           FROM {config.database.table_content}
           WHERE ({where_clause})
           ORDER BY {get_order_by_sql()} LIMIT ? OFFSET ?""",
        (per_page, offset),
    )
    return total, cur.fetchall()


@app.route("/")
def index() -> str:
    """首页列表"""
    page = max(1, int(request.args.get("page", 1)))
    search = (request.args.get("q") or "").strip()
    status_filter = (request.args.get("status") or "all").strip()
    per_page = min(
        config.display.per_page_max,
        max(10, int(request.args.get("per", config.display.per_page_default))),
    )

    try:
        conn = get_connection()
    except FileNotFoundError as e:
        return render_template("viewer_error.html", error=str(e)), 503

    offset = (page - 1) * per_page
    total, raw_rows = _execute_index_query(
        conn, "", status_filter, search, per_page, offset
    )
    conn.close()

    metas = [parse_meta(row[3]) for row in raw_rows]
    display_keys = _get_list_display_keys(metas)[: config.display.max_display_columns]

    rows = [
        _build_row_data(raw, meta, display_keys) for raw, meta in zip(raw_rows, metas)
    ]

    pages = max(1, (total + per_page - 1) // per_page)

    return render_template(
        "viewer_index.html",
        rows=rows,
        display_keys=display_keys,
        page=page,
        pages=pages,
        total=total,
        per_page=per_page,
        q=search,
        db_file=get_db_path(),
        status_filter=status_filter,
    )


@app.route("/item/<int:rid>")
def item(rid: int) -> str:
    """详情页"""
    try:
        conn = get_connection()
    except FileNotFoundError as e:
        return render_template("viewer_error.html", error=str(e)), 503

    cur = conn.execute(
        f"""SELECT id, description, updated_at, excel_meta,
           IFNULL(crawl_status,''), IFNULL(crawl_error,''),
           IFNULL(crawl_fail_count,0)
           FROM {config.database.table_content} WHERE id = ?""",
        (rid,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        abort(404)

    meta = parse_meta(row[3])
    meta_items = sorted(meta.items(), key=lambda x: x[0])
    stats = count_text_stats(row[1] or "")

    return render_template(
        "viewer_detail.html",
        rid=row[0],
        html_len=len(row[1] or ""),
        text_stats=stats,
        updated=format_timestamp(row[2]),
        render_url=url_for("render_html", rid=rid),
        raw_url=url_for("api_raw_html", rid=rid),
        meta_items=meta_items,
        crawl_status=row[4] or "—",
        crawl_error=row[5] or "",
        crawl_fail_count=row[6] or 0,
    )


@app.route("/api/raw/<int:rid>")
def api_raw_html(rid: int) -> Response:
    """获取原始HTML"""
    try:
        conn = get_connection()
    except FileNotFoundError:
        abort(503)

    cur = conn.execute(
        f"SELECT description FROM {config.database.table_content} WHERE id = ?", (rid,)
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
def render_html(rid: int) -> Response:
    """渲染HTML"""
    try:
        conn = get_connection()
    except FileNotFoundError:
        abort(503)

    cur = conn.execute(
        f"SELECT description FROM {config.database.table_content} WHERE id = ?", (rid,)
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


@app.route("/admin/clear", methods=["GET", "POST"])
def admin_clear() -> str:
    """清空数据管理"""
    confirm_word = "清空"

    try:
        conn = get_connection()
    except FileNotFoundError as e:
        return render_template("viewer_error.html", error=str(e)), 503

    if request.method == "POST":
        if (request.form.get("confirm") or "").strip() != confirm_word:
            conn.close()
            flash("验证失败：请在输入框中准确输入「清空」二字", "error")
            return redirect(url_for("admin_clear"))

        counts = clear_all_tables(conn)
        conn.close()

        main_count = counts.get(config.database.table_content, 0)
        flash(
            f"已清空：正文 {main_count} 条；URL 去重 / XPath / JSON 路径缓存已一并删除。",
            "success",
        )
        return redirect(url_for("index"))

    counts = get_table_counts(conn)
    conn.close()

    return render_template(
        "viewer_admin_clear.html",
        counts=counts,
        db_file=get_db_path(),
        confirm_word=confirm_word,
    )


@app.route("/health")
def health() -> dict[str, Any]:
    """健康检查"""
    return {"ok": True, "db": os.path.isfile(get_db_path())}


@app.route("/analytics")
def analytics_dashboard() -> str:
    """数据分析仪表盘"""
    try:
        report = get_full_analytics(get_db_path())
    except Exception as e:
        return render_template("viewer_error.html", error=str(e)), 500

    return render_template("viewer_analytics.html", report=report)


@app.route("/api/analytics/data")
def api_analytics_data() -> dict[str, Any]:
    """API: 获取分析数据"""
    try:
        return get_full_analytics(get_db_path())
    except Exception as e:
        return {"error": str(e)}, 500


@app.route("/api/analytics/daily-trend")
def api_daily_trend() -> dict[str, Any]:
    """API: 每日抓取趋势"""
    try:
        conn = get_connection()
        df = pd.read_sql_query(
            f"""SELECT updated_at, crawl_status 
               FROM {config.database.table_content}
               WHERE updated_at IS NOT NULL""",
            conn,
        )
        conn.close()

        df["date"] = df["updated_at"].apply(_parse_date_from_ts)
        df = df[df["date"].notna()]

        daily = df.groupby(["date", "crawl_status"]).size().unstack(fill_value=0)
        daily["total"] = daily.sum(axis=1)

        return {
            "dates": daily.index.tolist(),
            "total": daily.get("total", [0] * len(daily)).tolist(),
            "success": daily.get("ok", [0] * len(daily)).tolist(),
            "failed": daily.get("failed", [0] * len(daily)).tolist(),
        }
    except Exception as e:
        return {"error": str(e)}, 500


def _parse_date_from_ts(ts: Any) -> Optional[str]:
    """从时间戳解析日期"""
    if ts is None:
        return None
    try:
        ts_val = float(ts)
        if ts_val > 1e12:
            ts_val = ts_val / 1000.0
        return datetime.fromtimestamp(ts_val).strftime("%Y-%m-%d")
    except (TypeError, ValueError):
        return None


@app.route("/api/analytics/domain-stats")
def api_domain_stats() -> dict[str, Any]:
    """API: 域名统计"""
    try:
        conn = get_connection()
        df = pd.read_sql_query(
            f"""SELECT excel_meta, crawl_status 
               FROM {config.database.table_content}""",
            conn,
        )
        conn.close()

        df["domain"] = df["excel_meta"].apply(
            lambda x: parse_meta(x).get("主域名", "未知")
        )

        domain_stats = (
            df.groupby(["domain", "crawl_status"]).size().unstack(fill_value=0)
        )
        domain_stats["total"] = domain_stats.sum(axis=1)
        domain_stats = domain_stats.sort_values("total", ascending=False).head(15)

        return {
            "domains": domain_stats.index.tolist(),
            "total": domain_stats.get("total", [0] * len(domain_stats)).tolist(),
            "success": domain_stats.get("ok", [0] * len(domain_stats)).tolist(),
            "failed": domain_stats.get("failed", [0] * len(domain_stats)).tolist(),
        }
    except Exception as e:
        return {"error": str(e)}, 500


@app.route("/api/analytics/content-length")
def api_content_length() -> dict[str, Any]:
    """API: 内容长度分布"""
    try:
        conn = get_connection()
        df = pd.read_sql_query(
            f"""SELECT description 
               FROM {config.database.table_content}
               WHERE description IS NOT NULL""",
            conn,
        )
        conn.close()

        df["length"] = df["description"].apply(
            lambda x: count_text_stats(x)["total"] if x else 0
        )

        bins = [0, 100, 500, 1000, 5000, float("inf")]
        labels = ["0-100", "100-500", "500-1000", "1000-5000", "5000+"]
        df["range"] = pd.cut(df["length"], bins=bins, labels=labels, right=False)

        dist = df["range"].value_counts().sort_index()

        return {"ranges": labels, "counts": [int(dist.get(r, 0)) for r in labels]}
    except Exception as e:
        return {"error": str(e)}, 500


@app.route("/export")
def export_page() -> str:
    """数据导出页面"""
    try:
        conn = get_connection()

        domains = set()
        cur = conn.execute(
            f"SELECT excel_meta FROM {config.database.table_content} "
            f"WHERE excel_meta IS NOT NULL"
        )
        for row in cur.fetchall():
            meta = parse_meta(row[0])
            domains.add(meta.get("主域名", "未知"))
        conn.close()

        return render_template("viewer_export.html", domains=sorted(domains))
    except Exception as e:
        return render_template("viewer_error.html", error=str(e)), 500


@app.route("/export/excel")
def export_excel() -> Response:
    """导出Excel"""
    try:
        status = request.args.get("status", "all")
        domain = request.args.get("domain", "all")
        date_from = request.args.get("date_from")
        date_to = request.args.get("date_to")

        df = get_data_for_export(
            get_db_path(),
            status,
            domain if domain != "all" else None,
            date_from,
            date_to,
        )

        if df.empty:
            flash("没有符合条件的数据", "error")
            return redirect(url_for("export_page"))

        export_data = _prepare_export_data(df)
        export_df = pd.DataFrame(export_data)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            export_df.to_excel(writer, index=False, sheet_name="招投标数据")
        output.seek(0)

        filename = f"招投标数据_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=filename,
        )
    except Exception as e:
        flash(f"导出失败: {str(e)}", "error")
        return redirect(url_for("export_page"))


@app.route("/export/csv")
def export_csv() -> Response:
    """导出CSV"""
    try:
        status = request.args.get("status", "all")
        domain = request.args.get("domain", "all")
        date_from = request.args.get("date_from")
        date_to = request.args.get("date_to")

        df = get_data_for_export(
            get_db_path(),
            status,
            domain if domain != "all" else None,
            date_from,
            date_to,
        )

        if df.empty:
            flash("没有符合条件的数据", "error")
            return redirect(url_for("export_page"))

        export_data = _prepare_export_data(df)
        export_df = pd.DataFrame(export_data)

        output = io.StringIO()
        export_df.to_csv(output, index=False, encoding="utf-8-sig")
        output.seek(0)

        filename = f"招投标数据_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return Response(
            output.getvalue(),
            mimetype="text/csv; charset=utf-8-sig",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except Exception as e:
        flash(f"导出失败: {str(e)}", "error")
        return redirect(url_for("export_page"))


def _prepare_export_data(df: pd.DataFrame) -> list[dict[str, Any]]:
    """准备导出数据"""
    export_data = []
    for _, row in df.iterrows():
        meta = row.get("meta_dict", {})
        parsed_time = row.get("parsed_time")

        export_data.append(
            {
                "ID": row["id"],
                "标题": meta.get("标题", ""),
                "主域名": meta.get("主域名", ""),
                "来源": meta.get("来源", ""),
                "抓取状态": row["crawl_status"] or "ok",
                "失败次数": row["crawl_fail_count"],
                "错误信息": row["crawl_error"] or "",
                "更新时间": (
                    parsed_time.strftime("%Y-%m-%d %H:%M:%S") if parsed_time else ""
                ),
            }
        )
    return export_data


@app.route("/export/report")
def export_report() -> Response:
    """导出分析报告"""
    try:
        report = get_full_analytics(get_db_path())
        html_content = render_template(
            "viewer_report.html", report=report, generated_at=datetime.now()
        )

        filename = f"数据分析报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        return Response(
            html_content,
            mimetype="text/html; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except Exception as e:
        flash(f"报告生成失败: {str(e)}", "error")
        return redirect(url_for("analytics_dashboard"))


if __name__ == "__main__":
    print("数据库:", get_db_path())
    print(f"打开 http://{config.flask.host}:{config.flask.port}")
    app.run(host=config.flask.host, port=config.flask.port, debug=config.flask.debug)
