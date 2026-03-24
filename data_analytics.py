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
from typing import Any

import pandas as pd


def get_conn(db_path: str = "crawl_local.db") -> sqlite3.Connection:
    """获取数据库连接"""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def parse_meta(raw: str | None) -> dict:
    """解析 Excel 元数据 JSON"""
    if not raw:
        return {}
    try:
        d = json.loads(raw)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def get_content_length_stats(df: pd.DataFrame) -> dict:
    """分析内容长度分布"""
    def calc_content_length(html: str | None) -> int:
        if not html:
            return 0
        plain = re.sub(r"<[^>]+>", " ", html)
        plain = re.sub(r"\s+", " ", plain)
        return len(re.findall(r"[\u4e00-\u9fff]", plain)) + len(re.findall(r"\d", plain)) + len(re.findall(r"[A-Za-z]", plain))
    
    df["content_length"] = df["description"].apply(calc_content_length)
    
    return {
        "total_records": len(df),
        "avg_length": round(df["content_length"].mean(), 2),
        "median_length": round(df["content_length"].median(), 2),
        "min_length": int(df["content_length"].min()),
        "max_length": int(df["content_length"].max()),
        "std_length": round(df["content_length"].std(), 2),
        "length_distribution": {
            "0-100": int(((df["content_length"] >= 0) & (df["content_length"] < 100)).sum()),
            "100-500": int(((df["content_length"] >= 100) & (df["content_length"] < 500)).sum()),
            "500-1000": int(((df["content_length"] >= 500) & (df["content_length"] < 1000)).sum()),
            "1000-5000": int(((df["content_length"] >= 1000) & (df["content_length"] < 5000)).sum()),
            "5000+": int((df["content_length"] >= 5000).sum()),
        }
    }


def get_domain_stats(df: pd.DataFrame) -> dict:
    """按域名统计"""
    domains = []
    for _, row in df.iterrows():
        meta = parse_meta(row.get("excel_meta", ""))
        domain = meta.get("主域名", "未知")
        domains.append(domain)
    
    df["domain"] = domains
    domain_counts = df["domain"].value_counts().head(20).to_dict()
    
    return {
        "total_domains": df["domain"].nunique(),
        "top_domains": domain_counts,
        "domain_distribution": domain_counts
    }


def get_time_stats(df: pd.DataFrame) -> dict:
    """按时间统计"""
    def parse_timestamp(ts) -> datetime | None:
        if ts is None:
            return None
        try:
            ts_val = float(ts)
            if ts_val > 1e12:
                ts_val = ts_val / 1000.0
            return datetime.fromtimestamp(ts_val)
        except:
            return None
    
    df["parsed_time"] = df["updated_at"].apply(parse_timestamp)
    df["date"] = df["parsed_time"].apply(lambda x: x.strftime("%Y-%m-%d") if x else None)
    
    daily_counts = df[df["date"].notna()]["date"].value_counts().sort_index()
    
    # 最近7天和30天的数据
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
        }
    }


def get_crawl_quality_report(df: pd.DataFrame) -> dict:
    """生成数据质量报告"""
    total = len(df)
    
    # 抓取状态统计
    status_counts = df["crawl_status"].fillna("unknown").value_counts().to_dict()
    
    # 成功和失败统计
    success_count = status_counts.get("ok", 0) + status_counts.get("", 0)
    failed_count = status_counts.get("failed", 0)
    retrying_count = status_counts.get("retrying", 0)
    
    # 失败原因分类
    error_patterns = {
        "timeout": r"timeout|timed out|连接超时",
        "network": r"network|connection|connect|网络|连接",
        "http_error": r"404|403|500|502|503|HTTP",
        "parse_error": r"parse|解析|extract",
        "content_error": r"content|empty|内容",
    }
    
    error_classification = {key: 0 for key in error_patterns}
    error_classification["other"] = 0
    
    for error in df["crawl_error"].dropna():
        error_lower = str(error).lower()
        matched = False
        for category, pattern in error_patterns.items():
            if re.search(pattern, error_lower, re.IGNORECASE):
                error_classification[category] += 1
                matched = True
                break
        if not matched:
            error_classification["other"] += 1
    
    # 重试次数统计
    fail_count_dist = df["crawl_fail_count"].fillna(0).value_counts().sort_index().to_dict()
    
    success_rate = round(success_count / total * 100, 2) if total > 0 else 0
    
    return {
        "total_records": total,
        "success_count": int(success_count),
        "failed_count": int(failed_count),
        "retrying_count": int(retrying_count),
        "success_rate": success_rate,
        "status_distribution": status_counts,
        "error_classification": error_classification,
        "retry_distribution": {str(k): int(v) for k, v in fail_count_dist.items()},
    }


def get_full_analytics(db_path: str = "crawl_local.db") -> dict:
    """获取完整的数据分析报告"""
    conn = get_conn(db_path)
    
    # 读取所有数据
    df = pd.read_sql_query("""
        SELECT id, description, updated_at, excel_meta,
               IFNULL(crawl_status, '') AS crawl_status,
               IFNULL(crawl_error, '') AS crawl_error,
               IFNULL(crawl_fail_count, 0) AS crawl_fail_count
        FROM cms_crawl_data_content
    """, conn)
    
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


def get_data_for_export(
    db_path: str = "crawl_local.db",
    status_filter: str = None,
    domain_filter: str = None,
    date_from: str = None,
    date_to: str = None
) -> pd.DataFrame:
    """根据条件筛选数据用于导出"""
    conn = get_conn(db_path)
    
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
    conn.close()
    
    # 解析元数据
    df["meta_dict"] = df["excel_meta"].apply(parse_meta)
    df["domain"] = df["meta_dict"].apply(lambda x: x.get("主域名", "未知"))
    df["title"] = df["meta_dict"].apply(lambda x: x.get("标题", ""))
    df["source"] = df["meta_dict"].apply(lambda x: x.get("来源", ""))
    
    # 域名过滤
    if domain_filter and domain_filter != "all":
        df = df[df["domain"] == domain_filter]
    
    # 时间过滤
    def parse_ts(ts):
        if ts is None:
            return None
        try:
            ts_val = float(ts)
            if ts_val > 1e12:
                ts_val = ts_val / 1000.0
            return datetime.fromtimestamp(ts_val)
        except:
            return None
    
    df["parsed_time"] = df["updated_at"].apply(parse_ts)
    
    if date_from:
        df = df[df["parsed_time"] >= datetime.fromisoformat(date_from)]
    if date_to:
        df = df[df["parsed_time"] <= datetime.fromisoformat(date_to)]
    
    return df


if __name__ == "__main__":
    # 测试分析功能
    report = get_full_analytics()
    print(json.dumps(report, ensure_ascii=False, indent=2))
