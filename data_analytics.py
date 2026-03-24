# -*- coding: utf-8 -*-
"""
招投标数据分析模块
提供数据探索、统计分析和报告生成功能
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Optional

import pandas as pd

from config import ERROR_PATTERNS
from utils import (
    calc_content_length,
    get_db_connection,
    parse_meta,
    parse_timestamp,
)


# =============================================================================
# 内容长度分析
# =============================================================================
def get_content_length_stats(df: pd.DataFrame) -> dict[str, Any]:
    """分析内容长度分布"""
    df["content_length"] = df["description"].apply(calc_content_length)

    return {
        "total_records": len(df),
        "avg_length": round(df["content_length"].mean(), 2),
        "median_length": round(df["content_length"].median(), 2),
        "min_length": int(df["content_length"].min()),
        "max_length": int(df["content_length"].max()),
        "std_length": round(df["content_length"].std(), 2),
        "length_distribution": _get_length_distribution(df),
    }


def _get_length_distribution(df: pd.DataFrame) -> dict[str, int]:
    """获取内容长度分布统计"""
    return {
        "0-100": int(
            ((df["content_length"] >= 0) & (df["content_length"] < 100)).sum()
        ),
        "100-500": int(
            ((df["content_length"] >= 100) & (df["content_length"] < 500)).sum()
        ),
        "500-1000": int(
            ((df["content_length"] >= 500) & (df["content_length"] < 1000)).sum()
        ),
        "1000-5000": int(
            ((df["content_length"] >= 1000) & (df["content_length"] < 5000)).sum()
        ),
        "5000+": int((df["content_length"] >= 5000).sum()),
    }


# =============================================================================
# 域名统计
# =============================================================================
def get_domain_stats(df: pd.DataFrame) -> dict[str, Any]:
    """按域名统计"""
    df["domain"] = df["excel_meta"].apply(_extract_domain)
    domain_counts = df["domain"].value_counts().head(20).to_dict()

    return {
        "total_domains": df["domain"].nunique(),
        "top_domains": domain_counts,
        "domain_distribution": domain_counts,
    }


def _extract_domain(meta_raw: str) -> str:
    """从元数据中提取域名"""
    meta = parse_meta(meta_raw)
    return meta.get("主域名", "未知")


# =============================================================================
# 时间统计
# =============================================================================
def get_time_stats(df: pd.DataFrame) -> dict[str, Any]:
    """按时间统计"""
    df["parsed_time"] = df["updated_at"].apply(parse_timestamp)
    df["date"] = df["parsed_time"].apply(_format_date)

    daily_counts = df[df["date"].notna()]["date"].value_counts().sort_index()

    return {
        "total_days": df["date"].nunique(),
        "daily_trend": daily_counts.to_dict(),
        "recent_7d_count": _get_recent_count(df, 7),
        "recent_30d_count": _get_recent_count(df, 30),
        "date_range": _get_date_range(df),
    }


def _format_date(dt: Optional[datetime]) -> Optional[str]:
    """格式化日期"""
    if pd.isna(dt) or dt is None:
        return None
    return dt.strftime("%Y-%m-%d")


def _get_recent_count(df: pd.DataFrame, days: int) -> int:
    """获取最近 N 天的数据量"""
    cutoff = datetime.now() - timedelta(days=days)
    return len(df[df["parsed_time"] >= cutoff])


def _get_date_range(df: pd.DataFrame) -> dict[str, Optional[str]]:
    """获取日期范围"""
    valid_dates = df["date"].dropna()
    if valid_dates.empty:
        return {"start": None, "end": None}
    return {
        "start": valid_dates.min(),
        "end": valid_dates.max(),
    }


# =============================================================================
# 抓取质量报告
# =============================================================================
def get_crawl_quality_report(df: pd.DataFrame) -> dict[str, Any]:
    """生成数据质量报告"""
    total = len(df)
    status_counts = _get_status_counts(df)

    success_count = status_counts.get("ok", 0) + status_counts.get("", 0)
    failed_count = status_counts.get("failed", 0)
    retrying_count = status_counts.get("retrying", 0)

    success_rate = round(success_count / total * 100, 2) if total > 0 else 0

    return {
        "total_records": total,
        "success_count": int(success_count),
        "failed_count": int(failed_count),
        "retrying_count": int(retrying_count),
        "success_rate": success_rate,
        "status_distribution": status_counts,
        "error_classification": _classify_errors(df),
        "retry_distribution": _get_retry_distribution(df),
    }


def _get_status_counts(df: pd.DataFrame) -> dict[str, int]:
    """获取状态统计"""
    return df["crawl_status"].fillna("unknown").value_counts().to_dict()


def _classify_errors(df: pd.DataFrame) -> dict[str, int]:
    """分类错误信息"""
    error_classification = {key: 0 for key in ERROR_PATTERNS}
    error_classification["other"] = 0

    for error in df["crawl_error"].dropna():
        error_lower = str(error).lower()
        matched = False
        for category, pattern in ERROR_PATTERNS.items():
            if re.search(pattern, error_lower, re.IGNORECASE):
                error_classification[category] += 1
                matched = True
                break
        if not matched:
            error_classification["other"] += 1

    return error_classification


def _get_retry_distribution(df: pd.DataFrame) -> dict[str, int]:
    """获取重试次数分布"""
    # 先转换为数值类型，填充 None 为 0
    values = pd.to_numeric(df["crawl_fail_count"], errors="coerce").fillna(0)
    dist = values.value_counts().sort_index().to_dict()
    return {str(int(k)): int(v) for k, v in dist.items()}


# =============================================================================
# 完整数据分析报告
# =============================================================================
def get_full_analytics(db_path: Optional[str] = None) -> dict[str, Any]:
    """获取完整的数据分析报告"""
    conn = get_db_connection(db_path)

    df = pd.read_sql_query(
        """
        SELECT id, description, updated_at, excel_meta,
               IFNULL(crawl_status, '') AS crawl_status,
               IFNULL(crawl_error, '') AS crawl_error,
               IFNULL(crawl_fail_count, 0) AS crawl_fail_count
        FROM cms_crawl_data_content
        """,
        conn,
    )

    conn.close()

    if df.empty:
        return {"error": "数据库为空"}

    return {
        "content_stats": get_content_length_stats(df),
        "domain_stats": get_domain_stats(df),
        "time_stats": get_time_stats(df),
        "quality_report": get_crawl_quality_report(df),
        "generated_at": datetime.now().isoformat(),
    }


# =============================================================================
# 数据导出功能
# =============================================================================
def get_data_for_export(
    db_path: Optional[str] = None,
    status_filter: Optional[str] = None,
    domain_filter: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> pd.DataFrame:
    """根据条件筛选数据用于导出"""
    conn = get_db_connection(db_path)

    query = _build_export_query(status_filter)
    df = pd.read_sql_query(query, conn)
    conn.close()

    df = _enrich_with_metadata(df)
    df = _apply_domain_filter(df, domain_filter)
    df = _apply_date_filter(df, date_from, date_to)

    return df


def _build_export_query(status_filter: Optional[str]) -> str:
    """构建导出查询 SQL"""
    base_query = """
        SELECT id, description, updated_at, excel_meta,
               IFNULL(crawl_status, '') AS crawl_status,
               IFNULL(crawl_error, '') AS crawl_error,
               IFNULL(crawl_fail_count, 0) AS crawl_fail_count
        FROM cms_crawl_data_content
        WHERE 1=1
    """

    if status_filter and status_filter != "all":
        if status_filter == "ok":
            base_query += " AND (crawl_status = 'ok' OR crawl_status IS NULL OR crawl_status = '')"
        elif status_filter == "failed":
            base_query += " AND crawl_status = 'failed'"
        elif status_filter == "retrying":
            base_query += " AND crawl_status = 'retrying'"

    return base_query


def _enrich_with_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """丰富元数据信息"""
    df["meta_dict"] = df["excel_meta"].apply(parse_meta)
    df["domain"] = df["meta_dict"].apply(lambda x: x.get("主域名", "未知"))
    df["title"] = df["meta_dict"].apply(lambda x: x.get("标题", ""))
    df["source"] = df["meta_dict"].apply(lambda x: x.get("来源", ""))
    return df


def _apply_domain_filter(
    df: pd.DataFrame, domain_filter: Optional[str]
) -> pd.DataFrame:
    """应用域名过滤"""
    if domain_filter and domain_filter != "all":
        df = df[df["domain"] == domain_filter]
    return df


def _apply_date_filter(
    df: pd.DataFrame, date_from: Optional[str], date_to: Optional[str]
) -> pd.DataFrame:
    """应用日期过滤"""
    df["parsed_time"] = df["updated_at"].apply(parse_timestamp)

    if date_from:
        df = df[df["parsed_time"] >= datetime.fromisoformat(date_from)]
    if date_to:
        df = df[df["parsed_time"] <= datetime.fromisoformat(date_to)]

    return df


if __name__ == "__main__":
    report = get_full_analytics()
    print(json.dumps(report, ensure_ascii=False, indent=2))
