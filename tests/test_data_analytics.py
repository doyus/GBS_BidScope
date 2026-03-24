# -*- coding: utf-8 -*-
"""
data_analytics模块单元测试
"""
import time

import pandas as pd

from data_analytics import (
    get_content_length_distribution,
    get_domain_statistics,
    get_time_statistics,
    get_crawl_quality_report,
    _classify_errors,
    _get_retry_distribution,
    _build_export_query,
    _process_export_dataframe,
)


class TestGetContentLengthDistribution:
    """get_content_length_distribution函数测试"""

    def test_basic_distribution(self) -> None:
        """测试基本分布"""
        df = pd.DataFrame(
            {
                "description": [
                    "中文测试内容" * 10,
                    "短内容",
                    "A" * 200,
                ]
            }
        )

        result = get_content_length_distribution(df)

        assert result["total_records"] == 3
        assert "avg_length" in result
        assert "length_distribution" in result

    def test_empty_dataframe(self) -> None:
        """测试空数据框"""
        df = pd.DataFrame({"description": []})

        result = get_content_length_distribution(df)

        assert result["total_records"] == 0
        assert result["avg_length"] == 0
        assert result["min_length"] == 0
        assert result["max_length"] == 0


class TestGetDomainStatistics:
    """get_domain_statistics函数测试"""

    def test_domain_counts(self) -> None:
        """测试域名计数"""
        df = pd.DataFrame(
            {
                "excel_meta": [
                    '{"主域名": "example.com"}',
                    '{"主域名": "example.com"}',
                    '{"主域名": "test.com"}',
                ]
            }
        )

        result = get_domain_statistics(df)

        assert result["total_domains"] == 2
        assert "example.com" in result["top_domains"]

    def test_missing_meta(self) -> None:
        """测试缺失元数据"""
        df = pd.DataFrame({"excel_meta": [None, "", '{"other": "value"}']})

        result = get_domain_statistics(df)

        assert result["total_domains"] >= 1


class TestGetTimeStatistics:
    """get_time_statistics函数测试"""

    def test_time_stats(self) -> None:
        """测试时间统计"""
        now = time.time()
        df = pd.DataFrame({"updated_at": [now, now - 86400, now - 86400 * 2]})

        result = get_time_statistics(df)

        assert result["total_days"] == 3
        assert "daily_trend" in result

    def test_none_timestamps(self) -> None:
        """测试None时间戳"""
        df = pd.DataFrame({"updated_at": [None, None]})

        result = get_time_statistics(df)

        assert "daily_trend" in result


class TestGetCrawlQualityReport:
    """get_crawl_quality_report函数测试"""

    def test_quality_report(self) -> None:
        """测试质量报告"""
        df = pd.DataFrame(
            {
                "crawl_status": ["ok", "ok", "failed", "retrying"],
                "crawl_error": [None, None, "timeout", None],
                "crawl_fail_count": [0, 0, 1, 2],
            }
        )

        result = get_crawl_quality_report(df)

        assert result["total_records"] == 4
        assert result["success_count"] == 2
        assert result["failed_count"] == 1
        assert "success_rate" in result

    def test_error_classification(self) -> None:
        """测试错误分类"""
        df = pd.DataFrame(
            {
                "crawl_status": ["failed"] * 3,
                "crawl_error": ["timeout error", "network connection", "404 not found"],
                "crawl_fail_count": [1, 1, 1],
            }
        )

        result = get_crawl_quality_report(df)

        assert "error_classification" in result
        assert result["error_classification"]["timeout"] >= 1


class TestClassifyErrors:
    """_classify_errors函数测试"""

    def test_timeout_errors(self) -> None:
        """测试超时错误"""
        errors = pd.Series(["timeout", "timed out", "连接超时"])
        result = _classify_errors(errors)

        assert result["timeout"] == 3

    def test_network_errors(self) -> None:
        """测试网络错误"""
        errors = pd.Series(["network error", "connection refused"])
        result = _classify_errors(errors)

        assert result["network"] == 2

    def test_http_errors(self) -> None:
        """测试HTTP错误"""
        errors = pd.Series(["404 not found", "500 server error"])
        result = _classify_errors(errors)

        assert result["http_error"] == 2


class TestGetRetryDistribution:
    """_get_retry_distribution函数测试"""

    def test_retry_distribution(self) -> None:
        """测试重试分布"""
        fail_counts = pd.Series([0, 0, 1, 1, 1, 2])
        result = _get_retry_distribution(fail_counts)

        assert result["0"] == 2
        assert result["1"] == 3
        assert result["2"] == 1


class TestBuildExportQuery:
    """_build_export_query函数测试"""

    def test_all_status(self) -> None:
        """测试全部状态"""
        query, params = _build_export_query("all")

        assert "SELECT" in query
        assert len(params) == 0

    def test_ok_status(self) -> None:
        """测试成功状态"""
        query, params = _build_export_query("ok")

        assert "crawl_status = 'ok'" in query

    def test_failed_status(self) -> None:
        """测试失败状态"""
        query, params = _build_export_query("failed")

        assert "crawl_status = 'failed'" in query


class TestProcessExportDataframe:
    """_process_export_dataframe函数测试"""

    def test_processes_meta(self) -> None:
        """测试处理元数据"""
        df = pd.DataFrame(
            {
                "excel_meta": ['{"标题": "测试标题", "主域名": "example.com"}'],
                "updated_at": [time.time()],
                "crawl_status": ["ok"],
                "crawl_error": [None],
                "crawl_fail_count": [0],
            }
        )

        result = _process_export_dataframe(df, None, None, None)

        assert "meta_dict" in result.columns
        assert "domain" in result.columns
        assert "title" in result.columns

    def test_domain_filter(self) -> None:
        """测试域名过滤"""
        df = pd.DataFrame(
            {
                "excel_meta": ['{"主域名": "keep.com"}', '{"主域名": "remove.com"}'],
                "updated_at": [time.time(), time.time()],
                "crawl_status": ["ok", "ok"],
                "crawl_error": [None, None],
                "crawl_fail_count": [0, 0],
            }
        )

        result = _process_export_dataframe(df, "keep.com", None, None)

        assert len(result) == 1
        assert result.iloc[0]["domain"] == "keep.com"
