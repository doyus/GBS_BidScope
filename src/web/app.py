# -*- coding: utf-8 -*-
"""Web应用主模块"""
from __future__ import annotations

import io
import os
from datetime import datetime

import pandas as pd
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

from src.analytics.data_analyzer import DataAnalyzer
from src.config import settings
from src.database.connection import get_connection
from src.database.schema import clear_tables, get_table_counts
from src.utils.text_utils import (
    count_text_stats,
    format_meta_for_display,
    parse_meta,
    short_url,
    strip_tags_preview,
    truncate_text,
)
from src.utils.time_utils import format_timestamp


def create_app() -> Flask:
    """创建Flask应用"""
    app = Flask(__name__, template_folder="../../templates")
    app.config["JSON_AS_ASCII"] = settings.web.json_as_ascii
    app.secret_key = settings.web.secret_key

    # 注册路由
    _register_routes(app)

    return app


def _register_routes(app: Flask) -> None:
    """注册路由"""

    @app.route("/")
    def index():
        """首页 - 数据列表"""
        page = max(1, int(request.args.get("page", 1)))
        q = (request.args.get("q") or "").strip()
        status_filter = (request.args.get("status") or "all").strip()
        per_page = min(100, max(10, int(request.args.get("per", 30))))

        try:
            rows, total, display_keys = _fetch_list_data(
                page, q, status_filter, per_page
            )
        except FileNotFoundError as e:
            return render_template("viewer_error.html", error=str(e)), 503

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
            db_file=settings.database.db_path,
            status_filter=status_filter,
        )

    @app.route("/item/<int:rid>")
    def item(rid: int):
        """详情页"""
        try:
            data = _fetch_item_data(rid)
        except FileNotFoundError:
            return render_template("viewer_error.html", error="数据库不存在"), 503

        if not data:
            abort(404)

        return render_template("viewer_detail.html", **data)

    @app.route("/api/raw/<int:rid>")
    def api_raw_html(rid: int):
        """API: 获取原始HTML"""
        html = _fetch_raw_html(rid)
        if html is None:
            abort(404)
        return Response(html, mimetype="text/plain; charset=utf-8")

    @app.route("/render/<int:rid>")
    def render_html(rid: int):
        """渲染HTML"""
        html = _fetch_raw_html(rid)
        if html is None:
            abort(404)
        return Response(
            html,
            mimetype="text/html; charset=utf-8",
            headers={"X-Frame-Options": "SAMEORIGIN"},
        )

    @app.route("/admin/clear", methods=["GET", "POST"])
    def admin_clear():
        """清空数据"""
        confirm_word = "清空"

        try:
            with get_connection() as conn:
                if request.method == "POST":
                    if (request.form.get("confirm") or "").strip() != confirm_word:
                        flash("验证失败：请在输入框中准确输入「清空」二字", "error")
                        return redirect(url_for("admin_clear"))

                    counts = clear_tables(conn)
                    main_n = counts.get("cms_crawl_data_content", 0)
                    flash(
                        f"已清空：正文 {main_n} 条；URL 去重 / XPath / JSON 路径缓存已一并删除。",
                        "success",
                    )
                    return redirect(url_for("index"))

                counts = get_table_counts(conn)
        except FileNotFoundError as e:
            return render_template("viewer_error.html", error=str(e)), 503

        return render_template(
            "viewer_admin_clear.html",
            counts=counts,
            db_file=settings.database.db_path,
            confirm_word=confirm_word,
        )

    @app.route("/health")
    def health():
        """健康检查"""
        return {"ok": True, "db": os.path.isfile(settings.database.db_path)}

    @app.route("/analytics")
    def analytics_dashboard():
        """数据分析仪表盘"""
        try:
            analyzer = DataAnalyzer()
            report = analyzer.get_full_report()
        except Exception as e:
            return render_template("viewer_error.html", error=str(e)), 500

        return render_template("viewer_analytics.html", report=report)

    @app.route("/api/analytics/data")
    def api_analytics_data():
        """API: 获取分析数据"""
        try:
            analyzer = DataAnalyzer()
            return analyzer.get_full_report()
        except Exception as e:
            return {"error": str(e)}, 500

    @app.route("/api/analytics/daily-trend")
    def api_daily_trend():
        """API: 每日趋势"""
        return _get_daily_trend()

    @app.route("/api/analytics/domain-stats")
    def api_domain_stats():
        """API: 域名统计"""
        return _get_domain_stats()

    @app.route("/api/analytics/content-length")
    def api_content_length():
        """API: 内容长度分布"""
        return _get_content_length_dist()

    @app.route("/export")
    def export_page():
        """导出页面"""
        try:
            domains = _get_all_domains()
            return render_template("viewer_export.html", domains=sorted(domains))
        except Exception as e:
            return render_template("viewer_error.html", error=str(e)), 500

    @app.route("/export/excel")
    def export_excel():
        """导出Excel"""
        return _export_data("excel")

    @app.route("/export/csv")
    def export_csv():
        """导出CSV"""
        return _export_data("csv")

    @app.route("/export/report")
    def export_report():
        """导出分析报告"""
        try:
            analyzer = DataAnalyzer()
            report = analyzer.get_full_report()
            html_content = render_template(
                "viewer_report.html", report=report, generated_at=datetime.now()
            )
            return Response(
                html_content,
                mimetype="text/html; charset=utf-8",
                headers={
                    "Content-Disposition": f"attachment; filename=数据分析报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
                },
            )
        except Exception as e:
            flash(f"报告生成失败: {str(e)}", "error")
            return redirect(url_for("analytics_dashboard"))


def _build_status_where(status: str) -> str:
    """构建状态筛选条件"""
    s = (status or "all").strip().lower()

    if s == "ok":
        return (
            " (IFNULL(crawl_status,'') IN ('ok','') "
            "AND (IFNULL(crawl_error,'')='' OR crawl_status='ok') "
            "AND (LENGTH(IFNULL(description,''))>80 OR crawl_status='ok')) "
        )
    if s == "failed":
        return " IFNULL(crawl_status,'') = 'failed' "
    if s == "retrying":
        return " IFNULL(crawl_status,'') = 'retrying' "
    if s == "problem":
        return (
            " (IFNULL(crawl_status,'') IN ('failed','retrying') "
            "OR (IFNULL(crawl_error,'')!='' AND IFNULL(crawl_status,'')!='ok')) "
        )
    return " 1=1 "


def _fetch_list_data(
    page: int, q: str, status_filter: str, per_page: int
) -> tuple[list[dict], int, list[str]]:
    """获取列表数据"""
    with get_connection() as conn:
        sw = _build_status_where(status_filter)
        offset = (page - 1) * per_page

        if q:
            return _fetch_search_results(conn, q, sw, per_page, offset)
        return _fetch_all_results(conn, sw, per_page, offset)


def _fetch_search_results(conn, q: str, sw: str, per_page: int, offset: int):
    """获取搜索结果"""
    cur = conn.cursor()

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
               ORDER BY updated_at DESC LIMIT ? OFFSET ?""",
            (int(q), per_page, offset),
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
               ORDER BY updated_at DESC LIMIT ? OFFSET ?""",
            (like, like, like, per_page, offset),
        )

    rows = _process_rows(cur.fetchall())
    display_keys = _get_display_keys([r[3] for r in cur.fetchall()])

    return rows, total, display_keys


def _fetch_all_results(conn, sw: str, per_page: int, offset: int):
    """获取所有结果"""
    cur = conn.cursor()

    cur.execute(f"SELECT COUNT(*) FROM cms_crawl_data_content WHERE ({sw})")
    total = cur.fetchone()[0]

    cur.execute(
        f"""SELECT id, description, updated_at, excel_meta,
           IFNULL(crawl_status,'') AS cs, IFNULL(crawl_error,'') AS ce,
           IFNULL(crawl_fail_count,0) AS cf
           FROM cms_crawl_data_content
           WHERE ({sw})
           ORDER BY updated_at DESC LIMIT ? OFFSET ?""",
        (per_page, offset),
    )

    raw_rows = cur.fetchall()
    metas = [parse_meta(r[3]) for r in raw_rows]
    display_keys = _get_display_keys(metas)
    rows = _process_rows(raw_rows, display_keys)

    return rows, total, display_keys


def _get_display_keys(metas: list[dict]) -> list[str]:
    """获取要显示的列"""
    all_keys: set[str] = set()
    for m in metas:
        all_keys.update(m.keys())

    ordered = [k for k in settings.LIST_META_PRIORITY if k in all_keys]
    rest = sorted(all_keys - set(ordered))

    # 最多显示8列
    max_keys = 8
    return ordered + rest[: max_keys - len(ordered)]


def _process_rows(raw_rows: list, display_keys: list[str] | None = None) -> list[dict]:
    """处理行数据"""
    rows = []

    for raw in raw_rows:
        rid, desc, ts, meta_raw = raw[0], raw[1], raw[2], raw[3]
        cstat, cerr, cfc = (raw[4] or ""), (raw[5] or ""), raw[6]
        desc = desc or ""

        meta = parse_meta(meta_raw)
        cells = _build_cells(meta, display_keys or [])
        stats = count_text_stats(desc)

        st_raw = (cstat or "").strip().lower()
        row_cls = (
            "row-failed"
            if st_raw == "failed"
            else "row-retrying" if st_raw == "retrying" else ""
        )

        rows.append(
            {
                "id": rid,
                "cells": cells,
                "preview": strip_tags_preview(desc),
                "text_total": stats["total"],
                "text_cn": stats["cn"],
                "text_digit": stats["digit"],
                "text_alpha": stats["alpha"],
                "updated": format_timestamp(ts),
                "meta_extra_count": max(0, len(meta) - len(display_keys or [])),
                "crawl_status": cstat or "—",
                "crawl_error_short": truncate_text(cerr, 56) or "—",
                "crawl_error_full": cerr,
                "crawl_fail_count": cfc,
                "status_class": row_cls,
            }
        )

    return rows


def _build_cells(meta: dict, display_keys: list[str]) -> dict[str, str]:
    """构建单元格数据"""
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
    return cells


def _fetch_item_data(rid: int) -> dict | None:
    """获取单项数据"""
    with get_connection() as conn:
        cur = conn.execute(
            """SELECT id, description, updated_at, excel_meta,
               IFNULL(crawl_status,''), IFNULL(crawl_error,''), IFNULL(crawl_fail_count,0)
               FROM cms_crawl_data_content WHERE id = ?""",
            (rid,),
        )
        row = cur.fetchone()

        if not row:
            return None

        _id, desc, ts, meta_raw = row[0], row[1], row[2], row[3]
        crawl_status, crawl_error, crawl_fail_count = row[4], row[5], row[6]
        desc = desc or ""
        meta = parse_meta(meta_raw)

        return {
            "rid": _id,
            "html_len": len(desc),
            "text_stats": count_text_stats(desc),
            "updated": format_timestamp(ts),
            "render_url": f"/render/{rid}",
            "raw_url": f"/api/raw/{rid}",
            "meta_items": sorted(meta.items(), key=lambda x: x[0]),
            "crawl_status": crawl_status or "—",
            "crawl_error": crawl_error or "",
            "crawl_fail_count": crawl_fail_count or 0,
        }


def _fetch_raw_html(rid: int) -> str | None:
    """获取原始HTML"""
    try:
        with get_connection() as conn:
            cur = conn.execute(
                "SELECT description FROM cms_crawl_data_content WHERE id = ?",
                (rid,),
            )
            row = cur.fetchone()

            if not row:
                return None

            html = row[0] or ""
            if isinstance(html, bytes):
                html = html.decode("utf-8", errors="replace")
            return html
    except FileNotFoundError:
        return None


def _get_daily_trend():
    """获取每日趋势"""
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(
                """SELECT updated_at, crawl_status
                   FROM cms_crawl_data_content
                   WHERE updated_at IS NOT NULL""",
                conn,
            )

        df["date"] = df["updated_at"].apply(lambda x: format_date(x) if x else None)
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


def _get_domain_stats():
    """获取域名统计"""
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(
                "SELECT excel_meta, crawl_status FROM cms_crawl_data_content",
                conn,
            )

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


def _get_content_length_dist():
    """获取内容长度分布"""
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(
                "SELECT description FROM cms_crawl_data_content WHERE description IS NOT NULL",
                conn,
            )

        df["length"] = df["description"].apply(lambda x: count_text_stats(x)["total"])

        bins = [0, 100, 500, 1000, 5000, float("inf")]
        labels = ["0-100", "100-500", "500-1000", "1000-5000", "5000+"]
        df["range"] = pd.cut(df["length"], bins=bins, labels=labels, right=False)

        dist = df["range"].value_counts().sort_index()

        return {
            "ranges": labels,
            "counts": [int(dist.get(r, 0)) for r in labels],
        }
    except Exception as e:
        return {"error": str(e)}, 500


def _get_all_domains() -> set[str]:
    """获取所有域名"""
    domains = set()
    with get_connection() as conn:
        cur = conn.execute(
            "SELECT excel_meta FROM cms_crawl_data_content WHERE excel_meta IS NOT NULL"
        )
        for row in cur.fetchall():
            meta = parse_meta(row[0])
            domain = meta.get("主域名", "未知")
            domains.add(domain)
    return domains


def _export_data(format_type: str):
    """导出数据"""
    status = request.args.get("status", "all")
    domain = request.args.get("domain", "all")
    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")

    try:
        df = _fetch_export_data(
            status, domain if domain != "all" else None, date_from, date_to
        )

        if df.empty:
            flash("没有符合条件的数据", "error")
            return redirect(url_for("export_page"))

        export_data = _prepare_export_data(df)
        export_df = pd.DataFrame(export_data)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if format_type == "excel":
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                export_df.to_excel(writer, index=False, sheet_name="招投标数据")
            output.seek(0)
            return send_file(
                output,
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                as_attachment=True,
                download_name=f"招投标数据_{timestamp}.xlsx",
            )
        else:  # csv
            output = io.StringIO()
            export_df.to_csv(output, index=False, encoding="utf-8-sig")
            output.seek(0)
            return Response(
                output.getvalue(),
                mimetype="text/csv; charset=utf-8-sig",
                headers={
                    "Content-Disposition": f"attachment; filename=招投标数据_{timestamp}.csv"
                },
            )
    except Exception as e:
        flash(f"导出失败: {str(e)}", "error")
        return redirect(url_for("export_page"))


def _fetch_export_data(
    status_filter: str,
    domain_filter: str | None,
    date_from: str | None,
    date_to: str | None,
) -> pd.DataFrame:
    """获取导出数据"""
    with get_connection() as conn:
        query = """
            SELECT id, description, updated_at, excel_meta,
                   IFNULL(crawl_status, '') AS crawl_status,
                   IFNULL(crawl_error, '') AS crawl_error,
                   IFNULL(crawl_fail_count, 0) AS crawl_fail_count
            FROM cms_crawl_data_content
            WHERE 1=1
        """
        params = []

        if status_filter and status_filter != "all":
            if status_filter == "ok":
                query += " AND (crawl_status = 'ok' OR crawl_status IS NULL OR crawl_status = '')"
            elif status_filter == "failed":
                query += " AND crawl_status = 'failed'"
            elif status_filter == "retrying":
                query += " AND crawl_status = 'retrying'"

        df = pd.read_sql_query(query, conn, params=params)

    # 解析元数据
    df["meta_dict"] = df["excel_meta"].apply(parse_meta)
    df["domain"] = df["meta_dict"].apply(lambda x: x.get("主域名", "未知"))
    df["title"] = df["meta_dict"].apply(lambda x: x.get("标题", ""))
    df["source"] = df["meta_dict"].apply(lambda x: x.get("来源", ""))

    # 域名过滤
    if domain_filter:
        df = df[df["domain"] == domain_filter]

    # 时间过滤
    df["parsed_time"] = df["updated_at"].apply(parse_timestamp)

    if date_from:
        df = df[df["parsed_time"] >= datetime.fromisoformat(date_from)]
    if date_to:
        df = df[df["parsed_time"] <= datetime.fromisoformat(date_to)]

    return df


def _prepare_export_data(df: pd.DataFrame) -> list[dict]:
    """准备导出数据"""
    export_data = []
    for _, row in df.iterrows():
        meta = row.get("meta_dict", {})
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
                    row["parsed_time"].strftime("%Y-%m-%d %H:%M:%S")
                    if row["parsed_time"]
                    else ""
                ),
            }
        )
    return export_data
