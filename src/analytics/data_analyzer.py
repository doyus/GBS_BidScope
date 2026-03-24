# -*- coding: utf-8 -*-
"""数据分析模块"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from src.config import settings
from src.database.connection import get_connection
from src.utils.text_utils import count_text_stats, parse_meta
from src.utils.time_utils import format_date, parse_timestamp


@dataclass
class ContentStats:
    """内容统计"""

    total_records: int
    avg_length: float
    median_length: float
    min_length: int
    max_length: int
    std_length: float
    length_distribution: dict[str, int]


@dataclass
class DomainStats:
    """域名统计"""

    total_domains: int
    top_domains: dict[str, int]


@dataclass
class TimeStats:
    """时间统计"""

    total_days: int
    daily_trend: dict[str, int]
    recent_7d_count: int
    recent_30d_count: int
    date_range: dict[str, str | None]


@dataclass
class QualityReport:
    """质量报告"""

    total_records: int
    success_count: int
    failed_count: int
    retrying_count: int
    success_rate: float
    status_distribution: dict[str, int]
    error_classification: dict[str, int]
    retry_distribution: dict[str, int]


class DataAnalyzer:
    """数据分析器"""

    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path or settings.database.db_path

    def _load_data(self) -> pd.DataFrame:
        """加载数据"""
        with get_connection() as conn:
            return pd.read_sql_query(
                """
                SELECT id, description, updated_at, excel_meta,
                       IFNULL(crawl_status, '') AS crawl_status,
                       IFNULL(crawl_error, '') AS crawl_error,
                       IFNULL(crawl_fail_count, 0) AS crawl_fail_count
                FROM cms_crawl_data_content
                """,
                conn,
            )

    def analyze_content_length(self, df: pd.DataFrame) -> ContentStats:
        """分析内容长度"""
        df = df.copy()
        df["content_length"] = df["description"].apply(
            lambda x: count_text_stats(x)["total"]
        )

        # 分布统计
        bins = [(0, 100), (100, 500), (500, 1000), (1000, 5000), (5000, float("inf"))]
        labels = ["0-100", "100-500", "500-1000", "1000-5000", "5000+"]
        distribution = {}

        for (low, high), label in zip(bins, labels):
            if high == float("inf"):
                count = int((df["content_length"] >= low).sum())
            else:
                count = int(
                    (
                        (df["content_length"] >= low) & (df["content_length"] < high)
                    ).sum()
                )
            distribution[label] = count

        return ContentStats(
            total_records=len(df),
            avg_length=round(df["content_length"].mean(), 2),
            median_length=round(df["content_length"].median(), 2),
            min_length=int(df["content_length"].min()),
            max_length=int(df["content_length"].max()),
            std_length=round(df["content_length"].std(), 2),
            length_distribution=distribution,
        )

    def analyze_domains(self, df: pd.DataFrame) -> DomainStats:
        """分析域名分布"""
        df = df.copy()
        df["domain"] = df["excel_meta"].apply(
            lambda x: parse_meta(x).get("主域名", "未知")
        )

        top_domains = df["domain"].value_counts().head(20).to_dict()

        return DomainStats(
            total_domains=df["domain"].nunique(),
            top_domains=top_domains,
        )

    def analyze_time(self, df: pd.DataFrame) -> TimeStats:
        """分析时间分布"""
        df = df.copy()
        df["parsed_time"] = df["updated_at"].apply(parse_timestamp)
        df["date"] = df["parsed_time"].apply(
            lambda x: x.strftime("%Y-%m-%d") if x else None
        )

        daily_counts = df[df["date"].notna()]["date"].value_counts().sort_index()

        # 最近7天和30天
        today = datetime.now()
        last_7d = today - timedelta(days=7)
        last_30d = today - timedelta(days=30)

        recent_7d = df[df["parsed_time"] >= last_7d]
        recent_30d = df[df["parsed_time"] >= last_30d]

        return TimeStats(
            total_days=df["date"].nunique(),
            daily_trend=daily_counts.to_dict(),
            recent_7d_count=len(recent_7d),
            recent_30d_count=len(recent_30d),
            date_range={
                "start": df["date"].min() if not df["date"].empty else None,
                "end": df["date"].max() if not df["date"].empty else None,
            },
        )

    def analyze_quality(self, df: pd.DataFrame) -> QualityReport:
        """分析数据质量"""
        total = len(df)

        # 状态统计
        status_counts = df["crawl_status"].fillna("unknown").value_counts().to_dict()

        success_count = status_counts.get("ok", 0) + status_counts.get("", 0)
        failed_count = status_counts.get("failed", 0)
        retrying_count = status_counts.get("retrying", 0)

        # 错误分类
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
                if (
                    pd.Series([error_lower])
                    .str.contains(pattern, case=False, regex=True)
                    .any()
                ):
                    error_classification[category] += 1
                    matched = True
                    break
            if not matched:
                error_classification["other"] += 1

        # 重试分布
        fail_count_dist = (
            df["crawl_fail_count"].fillna(0).value_counts().sort_index().to_dict()
        )

        success_rate = round(success_count / total * 100, 2) if total > 0 else 0

        return QualityReport(
            total_records=total,
            success_count=int(success_count),
            failed_count=int(failed_count),
            retrying_count=int(retrying_count),
            success_rate=success_rate,
            status_distribution=status_counts,
            error_classification=error_classification,
            retry_distribution={str(k): int(v) for k, v in fail_count_dist.items()},
        )

    def get_full_report(self) -> dict[str, Any]:
        """获取完整报告"""
        df = self._load_data()

        if df.empty:
            return {"error": "数据库为空"}

        content_stats = self.analyze_content_length(df)
        domain_stats = self.analyze_domains(df)
        time_stats = self.analyze_time(df)
        quality_report = self.analyze_quality(df)

        return {
            "content_stats": {
                "total_records": content_stats.total_records,
                "avg_length": content_stats.avg_length,
                "median_length": content_stats.median_length,
                "min_length": content_stats.min_length,
                "max_length": content_stats.max_length,
                "std_length": content_stats.std_length,
                "length_distribution": content_stats.length_distribution,
            },
            "domain_stats": {
                "total_domains": domain_stats.total_domains,
                "top_domains": domain_stats.top_domains,
            },
            "time_stats": {
                "total_days": time_stats.total_days,
                "daily_trend": time_stats.daily_trend,
                "recent_7d_count": time_stats.recent_7d_count,
                "recent_30d_count": time_stats.recent_30d_count,
                "date_range": time_stats.date_range,
            },
            "quality_report": {
                "total_records": quality_report.total_records,
                "success_count": quality_report.success_count,
                "failed_count": quality_report.failed_count,
                "retrying_count": quality_report.retrying_count,
                "success_rate": quality_report.success_rate,
                "status_distribution": quality_report.status_distribution,
                "error_classification": quality_report.error_classification,
                "retry_distribution": quality_report.retry_distribution,
            },
            "generated_at": datetime.now().isoformat(),
        }


def get_full_analytics(db_path: str | None = None) -> dict[str, Any]:
    """便捷函数：获取完整分析"""
    analyzer = DataAnalyzer(db_path)
    return analyzer.get_full_report()
