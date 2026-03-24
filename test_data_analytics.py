# -*- coding: utf-8 -*-
"""
单元测试：数据分析模块 data_analytics.py
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import datetime, timedelta

import pandas as pd
import pytest

from data_analytics import (
    _apply_date_filter,
    _apply_domain_filter,
    _build_export_query,
    _classify_errors,
    _enrich_with_metadata,
    _extract_domain,
    _format_date,
    _get_date_range,
    _get_length_distribution,
    _get_recent_count,
    _get_retry_distribution,
    _get_status_counts,
    get_content_length_stats,
    get_crawl_quality_report,
    get_data_for_export,
    get_domain_stats,
    get_full_analytics,
    get_time_stats,
)
from utils import init_sqlite


class TestContentLengthStats:
    """内容长度统计测试"""

    def test_get_content_length_stats(self):
        """测试内容长度统计"""
        df = pd.DataFrame(
            {
                "description": [
                    "<p>这是一段中文文本123ABC</p>",  # 长度：8中文 + 3数字 + 3字母 = 14
                    "<div>另一段测试新内容456DEF</div>",  # 8中文 + 3数字 + 3字母 = 14
                ]
            }
        )
        result = get_content_length_stats(df)
        assert result["total_records"] == 2
        assert result["avg_length"] == 14
        assert "length_distribution" in result

    def test_get_length_distribution(self):
        """测试长度分布统计"""
        df = pd.DataFrame({"content_length": [50, 200, 600, 2000, 6000]})
        result = _get_length_distribution(df)
        assert result["0-100"] == 1
        assert result["100-500"] == 1
        assert result["500-1000"] == 1
        assert result["1000-5000"] == 1
        assert result["5000+"] == 1


class TestDomainStats:
    """域名统计测试"""

    def test_extract_domain(self):
        """测试从元数据提取域名"""
        meta_raw = '{"主域名": "example.com"}'
        result = _extract_domain(meta_raw)
        assert result == "example.com"

    def test_extract_domain_unknown(self):
        """测试未知域名处理"""
        meta_raw = "{}"
        result = _extract_domain(meta_raw)
        assert result == "未知"

    def test_get_domain_stats(self):
        """测试域名统计"""
        df = pd.DataFrame(
            {
                "excel_meta": [
                    '{"主域名": "example.com"}',
                    '{"主域名": "test.com"}',
                    '{"主域名": "example.com"}',
                ]
            }
        )
        result = get_domain_stats(df)
        assert result["total_domains"] == 2
        assert "example.com" in result["top_domains"]
        assert result["top_domains"]["example.com"] == 2


class TestTimeStats:
    """时间统计测试"""

    def test_format_date(self):
        """测试日期格式化"""
        dt = datetime(2024, 1, 1, 12, 0, 0)
        result = _format_date(dt)
        assert result == "2024-01-01"

    def test_format_date_none(self):
        """测试空日期格式化"""
        assert _format_date(None) is None

    def test_get_recent_count(self):
        """测试最近天数统计"""
        now = datetime.now()
        dates = [
            now - timedelta(days=1),  # 最近7天内
            now - timedelta(days=5),  # 最近7天内
            now - timedelta(days=10),  # 超过7天
        ]
        df = pd.DataFrame({"parsed_time": dates})
        result = _get_recent_count(df, 7)
        assert result == 2

    def test_get_date_range(self):
        """测试日期范围"""
        df = pd.DataFrame({"date": ["2024-01-01", "2024-01-02", "2024-01-03"]})
        result = _get_date_range(df)
        assert result["start"] == "2024-01-01"
        assert result["end"] == "2024-01-03"

    def test_get_date_range_empty(self):
        """测试空数据日期范围"""
        df = pd.DataFrame({"date": []})
        result = _get_date_range(df)
        assert result["start"] is None
        assert result["end"] is None

    def test_get_time_stats(self):
        """测试时间统计"""
        now = datetime.now()
        df = pd.DataFrame(
            {
                "updated_at": [
                    now.timestamp(),
                    (now - timedelta(days=1)).timestamp(),
                    None,
                ]
            }
        )
        result = get_time_stats(df)
        assert "total_days" in result
        assert "daily_trend" in result
        assert "recent_7d_count" in result


class TestQualityReport:
    """质量报告测试"""

    def test_get_status_counts(self):
        """测试状态统计"""
        df = pd.DataFrame({"crawl_status": ["ok", "failed", "", "ok", "retrying", None]})
        result = _get_status_counts(df)
        assert result.get("ok", 0) == 2
        assert result.get("failed", 0) == 1
        assert "unknown" in result  # None 值会被填充为 unknown

    def test_classify_errors(self):
        """测试错误分类"""
        from config import ERROR_PATTERNS

        df = pd.DataFrame(
            {
                "crawl_error": [
                    "连接超时",
                    "network error",
                    "HTTP 404",
                    "解析失败",
                    "内容为空",
                    "unknown error",
                ]
            }
        )
        result = _classify_errors(df)
        assert result["timeout"] >= 1
        assert result["network"] >= 1
        assert result["http_error"] >= 1
        assert result["parse_error"] >= 1
        assert result["content_error"] >= 1
        assert result["other"] >= 1

    def test_get_retry_distribution(self):
        """测试重试分布"""
        df = pd.DataFrame({"crawl_fail_count": [0, 1, 2, 0, 1, None]})
        result = _get_retry_distribution(df)
        assert result["0"] == 3  # None 被填充为 0
        assert result["1"] == 2
        assert result["2"] == 1

    def test_get_crawl_quality_report(self):
        """测试抓取质量报告"""
        df = pd.DataFrame(
            {
                "crawl_status": ["ok", "failed", "ok", "retrying"],
                "crawl_error": ["", "连接超时", "", ""],
                "crawl_fail_count": [0, 1, 0, 2],
            }
        )
        result = get_crawl_quality_report(df)
        assert result["total_records"] == 4
        assert result["success_count"] == 2
        assert result["failed_count"] == 1
        assert "success_rate" in result


class TestFullAnalytics:
    """完整分析报告测试"""

    def test_get_full_analytics_empty(self):
        """测试空数据库分析报告"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            init_sqlite(db_path)
            result = get_full_analytics(db_path)
            assert "error" in result  # 空数据库应该返回错误信息
        finally:
            os.unlink(db_path)

    def test_get_full_analytics_with_data(self):
        """测试有数据的分析报告"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            init_sqlite(db_path)
            conn = sqlite3.connect(db_path)

            # 插入测试数据
            now = datetime.now().timestamp()
            conn.execute(
                """INSERT INTO cms_crawl_data_content 
                   (id, description, updated_at, excel_meta, crawl_status, crawl_error, crawl_fail_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    1,
                    "<p>测试内容</p>",
                    now,
                    '{"主域名": "example.com"}',
                    "ok",
                    "",
                    0,
                ),
            )
            conn.commit()
            conn.close()

            result = get_full_analytics(db_path)
            assert "content_stats" in result
            assert "domain_stats" in result
            assert "time_stats" in result
            assert "quality_report" in result
        finally:
            os.unlink(db_path)


class TestDataExport:
    """数据导出测试"""

    def test_build_export_query_all(self):
        """测试导出查询构建 - 全部状态"""
        result = _build_export_query("all")
        assert "WHERE 1=1" in result

    def test_build_export_query_ok(self):
        """测试导出查询构建 - 成功状态"""
        result = _build_export_query("ok")
        assert "crawl_status = 'ok'" in result

    def test_build_export_query_failed(self):
        """测试导出查询构建 - 失败状态"""
        result = _build_export_query("failed")
        assert "crawl_status = 'failed'" in result

    def test_build_export_query_retrying(self):
        """测试导出查询构建 - 重试状态"""
        result = _build_export_query("retrying")
        assert "crawl_status = 'retrying'" in result

    def test_enrich_with_metadata(self):
        """测试元数据丰富"""
        df = pd.DataFrame(
            {
                "excel_meta": [
                    '{"主域名": "example.com", "标题": "测试公告", "来源": "测试来源"}',
                    "{}",
                ],
            }
        )
        result = _enrich_with_metadata(df)
        assert "domain" in result.columns
        assert "title" in result.columns
        assert "source" in result.columns
        assert result.iloc[0]["domain"] == "example.com"
        assert result.iloc[1]["domain"] == "未知"

    def test_apply_domain_filter(self):
        """测试域名过滤"""
        df = pd.DataFrame({"domain": ["example.com", "test.com", "example.com"]})
        result = _apply_domain_filter(df, "example.com")
        assert len(result) == 2
        assert all(result["domain"] == "example.com")

    def test_apply_domain_filter_all(self):
        """测试不过滤域名"""
        df = pd.DataFrame({"domain": ["example.com", "test.com"]})
        result = _apply_domain_filter(df, "all")
        assert len(result) == 2

    def test_apply_date_filter(self):
        """测试日期过滤"""
        now = datetime.now()
        df = pd.DataFrame(
            {
                "updated_at": [
                    now.timestamp(),
                    (now - timedelta(days=10)).timestamp(),
                ],
            }
        )
        date_from = (now - timedelta(days=5)).strftime("%Y-%m-%d")
        result = _apply_date_filter(df, date_from, None)
        assert len(result) == 1

    def test_get_data_for_export(self):
        """测试数据导出"""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            init_sqlite(db_path)
            conn = sqlite3.connect(db_path)

            # 插入测试数据
            now = datetime.now().timestamp()
            conn.execute(
                """INSERT INTO cms_crawl_data_content 
                   (id, description, updated_at, excel_meta, crawl_status, crawl_error, crawl_fail_count)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    1,
                    "<p>测试内容</p>",
                    now,
                    '{"主域名": "example.com"}',
                    "ok",
                    "",
                    0,
                ),
            )
            conn.commit()
            conn.close()

            result = get_data_for_export(db_path)
            assert len(result) == 1
            assert "domain" in result.columns
        finally:
            os.unlink(db_path)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])