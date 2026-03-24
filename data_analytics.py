# -*- coding: utf-8 -*-
"""
招投标数据分析模块 - 提供数据探索、统计分析和报告生成功能
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, Optional

import pandas as pd

from config import config
from database import get_connection_simple, get_db_path
from utils import parse_meta, parse_timestamp, count_valid_text


def get_content_length_distribution(df: pd.DataFrame) -> dict[str, Any]:
    """分析内容长度分布"""
    if df.empty:
        return {
            "total_records": 0,
            "avg_length": 0.0,
            "median_length": 0.0,
            "min_length": 0,
            "max_length": 0,
            "std_length": 0.0,
            "length_distribution": {
                "0-100": 0,
                "100-500": 0,
                "500-1000": 0,
                "1000-5000": 0,
                "5000+": 0,
            },
        }

    df = df.copy()
    df["content_length"] = df["description"].apply(
        lambda x: count_valid_text(x) if x else 0
    )

    return {
        "total_records": len(df),
        "avg_length": round(df["content_length"].mean(), 2),
        "median_length": round(df["content_length"].median(), 2),
        "min_length": int(df["content_length"].min()),
        "max_length": int(df["content_length"].max()),
        "std_length": round(df["content_length"].std(), 2),
        "length_distribution": _calculate_length_bins(df["content_length"]),
    }


def _calculate_length_bins(lengths: pd.Series) -> dict[str, int]:
    """计算长度分布区间"""
    return {
        "0-100": int(((lengths >= 0) & (lengths < 100)).sum()),
        "100-500": int(((lengths >= 100) & (lengths < 500)).sum()),
        "500-1000": int(((lengths >= 500) & (lengths < 1000)).sum()),
        "1000-5000": int(((lengths >= 1000) & (lengths < 5000)).sum()),
        "5000+": int((lengths >= 5000).sum()),
    }


def get_domain_statistics(df: pd.DataFrame) -> dict[str, Any]:
    """按域名统计"""
    df = df.copy()
    df["domain"] = df["excel_meta"].apply(lambda x: parse_meta(x).get("主域名", "未知"))

    domain_counts = df["domain"].value_counts().head(20).to_dict()

    return {
        "total_domains": df["domain"].nunique(),
        "top_domains": domain_counts,
        "domain_distribution": domain_counts,
    }


def get_time_statistics(df: pd.DataFrame) -> dict[str, Any]:
    """按时间统计"""
    df = df.copy()
    df["parsed_time"] = df["updated_at"].apply(parse_timestamp)
    df["date"] = df["parsed_time"].apply(
        lambda x: x.strftime("%Y-%m-%d") if x else None
    )

    daily_counts = df[df["date"].notna()]["date"].value_counts().sort_index()

    today = datetime.now()
    last_7d = today - timedelta(days=7)
    last_30d = today - timedelta(days=30)

    recent_7d = df[df["parsed_time"] >= last_7d]
    recent_30d = df[df["parsed_time"] >= last_30d]

    return {
        "total_days": df["date"].nunique(),
        "daily_trend": daily_counts.to_dict(),
        "recent_7d_count": len(recent_7d),
        "recent_30d_count": len(recent_30d),
        "date_range": {
            "start": df["date"].min() if not df["date"].empty else None,
            "end": df["date"].max() if not df["date"].empty else None,
        },
    }


def get_crawl_quality_report(df: pd.DataFrame) -> dict[str, Any]:
    """生成数据质量报告"""
    total = len(df)

    status_counts = df["crawl_status"].fillna("unknown").value_counts().to_dict()

    success_count = status_counts.get("ok", 0) + status_counts.get("", 0)
    failed_count = status_counts.get("failed", 0)
    retrying_count = status_counts.get("retrying", 0)

    error_classification = _classify_errors(df["crawl_error"])
    retry_distribution = _get_retry_distribution(df["crawl_fail_count"])

    success_rate = round(success_count / total * 100, 2) if total > 0 else 0

    return {
        "total_records": total,
        "success_count": int(success_count),
        "failed_count": int(failed_count),
        "retrying_count": int(retrying_count),
        "success_rate": success_rate,
        "status_distribution": status_counts,
        "error_classification": error_classification,
        "retry_distribution": retry_distribution,
    }


def _classify_errors(errors: pd.Series) -> dict[str, int]:
    """分类错误类型"""
    error_patterns = {
        "timeout": r"timeout|timed out|连接超时",
        "network": r"network|connection|connect|网络|连接",
        "http_error": r"404|403|500|502|503|HTTP",
        "parse_error": r"parse|解析|extract",
        "content_error": r"content|empty|内容",
    }

    classification = {key: 0 for key in error_patterns}
    classification["other"] = 0

    for error in errors.dropna():
        error_lower = str(error).lower()
        matched = False

        for category, pattern in error_patterns.items():
            if re.search(pattern, error_lower, re.IGNORECASE):
                classification[category] += 1
                matched = True
                break

        if not matched:
            classification["other"] += 1

    return classification


def _get_retry_distribution(fail_counts: pd.Series) -> dict[str, int]:
    """获取重试次数分布"""
    dist = fail_counts.fillna(0).value_counts().sort_index().to_dict()
    return {str(k): int(v) for k, v in dist.items()}


def get_full_analytics(db_path: Optional[str] = None) -> dict[str, Any]:
    """获取完整的数据分析报告"""
    path = db_path or get_db_path()
    conn = get_connection_simple(path)

    try:
        df = pd.read_sql_query(
            f"""
            SELECT id, description, updated_at, excel_meta,
                   IFNULL(crawl_status, '') AS crawl_status,
                   IFNULL(crawl_error, '') AS crawl_error,
                   IFNULL(crawl_fail_count, 0) AS crawl_fail_count
            FROM {config.database.table_content}
            """,
            conn,
        )
    finally:
        conn.close()

    if df.empty:
        return {"error": "数据库为空"}

    return {
        "content_stats": get_content_length_distribution(df),
        "domain_stats": get_domain_statistics(df),
        "time_stats": get_time_statistics(df),
        "quality_report": get_crawl_quality_report(df),
        "generated_at": datetime.now().isoformat(),
    }


def get_data_for_export(
    db_path: Optional[str] = None,
    status_filter: Optional[str] = None,
    domain_filter: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> pd.DataFrame:
    """根据条件筛选数据用于导出"""
    path = db_path or get_db_path()
    conn = get_connection_simple(path)

    try:
        query, params = _build_export_query(status_filter)
        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

    if df.empty:
        return df

    df = _process_export_dataframe(df, domain_filter, date_from, date_to)
    return df


def _build_export_query(status_filter: Optional[str]) -> tuple[str, list]:
    """构建导出查询SQL"""
    query = f"""
        SELECT id, description, updated_at, excel_meta,
               IFNULL(crawl_status, '') AS crawl_status,
               IFNULL(crawl_error, '') AS crawl_error,
               IFNULL(crawl_fail_count, 0) AS crawl_fail_count
        FROM {config.database.table_content}
        WHERE 1=1
    """
    params: list = []

    if status_filter and status_filter != "all":
        if status_filter == "ok":
            query += " AND (crawl_status = 'ok' OR crawl_status IS NULL OR crawl_status = '')"
        elif status_filter == "failed":
            query += " AND crawl_status = 'failed'"
        elif status_filter == "retrying":
            query += " AND crawl_status = 'retrying'"

    return query, params


def _process_export_dataframe(
    df: pd.DataFrame,
    domain_filter: Optional[str],
    date_from: Optional[str],
    date_to: Optional[str],
) -> pd.DataFrame:
    """处理导出数据"""
    df["meta_dict"] = df["excel_meta"].apply(parse_meta)
    df["domain"] = df["meta_dict"].apply(lambda x: x.get("主域名", "未知"))
    df["title"] = df["meta_dict"].apply(lambda x: x.get("标题", ""))
    df["source"] = df["meta_dict"].apply(lambda x: x.get("来源", ""))

    if domain_filter and domain_filter != "all":
        df = df[df["domain"] == domain_filter]

    df["parsed_time"] = df["updated_at"].apply(parse_timestamp)

    if date_from:
        df = df[df["parsed_time"] >= datetime.fromisoformat(date_from)]
    if date_to:
        df = df[df["parsed_time"] <= datetime.fromisoformat(date_to)]

    return df


if __name__ == "__main__":
    import json

    report = get_full_analytics()
    print(json.dumps(report, ensure_ascii=False, indent=2))
