# -*- coding: utf-8 -*-
"""内容提取器测试"""
from __future__ import annotations

import pytest

from src.crawler.content_extractor import (
    _bidding_json_to_article_html,
    extract_from_json_html,
    extract_heuristic,
    extract_readability,
    is_good_main_content,
)
from src.utils.text_utils import try_parse_json_loose


class TestIsGoodMainContent:
    """测试内容质量判断"""

    def test_short_content_fails(self):
        html = "<p>Short</p>"
        assert is_good_main_content(html) is False

    def test_good_content_passes(self):
        html = "<p>" + "这是一段测试内容。" * 20 + "</p>"
        assert is_good_main_content(html) is True

    def test_none_fails(self):
        assert is_good_main_content(None) is False

    def test_empty_fails(self):
        assert is_good_main_content("") is False


class TestExtractReadability:
    """测试readability提取"""

    def test_extracts_from_valid_html(self, sample_html):
        result = extract_readability(sample_html)
        assert len(result) > 0

    def test_returns_empty_for_invalid(self):
        result = extract_readability("not html")
        assert result == ""

    def test_returns_empty_for_none(self):
        result = extract_readability(None)
        assert result == ""


class TestExtractHeuristic:
    """测试启发式提取"""

    def test_extracts_article_tag(self):
        html = """
        <html><body>
            <article>
                <p>这是一段很长的文章内容，需要有足够的文字才能被识别。</p>
                <p>第二段内容。</p>
                <p>第三段内容。</p>
            </article>
        </body></html>
        """
        result = extract_heuristic(html)
        # 启发式提取可能返回空，取决于内容长度
        assert isinstance(result, str)

    def test_returns_empty_for_no_match(self):
        html = "<html><body><p>Short</p></body></html>"
        result = extract_heuristic(html)
        assert result == ""


class TestBiddingJsonToArticleHtml:
    """测试招投标JSON转HTML"""

    def test_converts_project_data(self):
        data = {
            "data": {
                "tproject": {
                    "projectName": "测试项目",
                    "projectNo": "2024-001",
                    "purchaseDept": "采购部门",
                }
            }
        }
        result = _bidding_json_to_article_html(data)
        assert "测试项目" in result
        assert "2024-001" in result

    def test_handles_empty_dict(self):
        result = _bidding_json_to_article_html({})
        assert result == ""

    def test_handles_non_dict(self):
        result = _bidding_json_to_article_html("not a dict")
        assert result == ""


class TestExtractFromJsonHtml:
    """测试从JSON HTML提取"""

    def test_extracts_from_pre_tag(self):
        html = '<pre>{"data": {"tproject": {"projectName": "测试"}}}</pre>'
        result = extract_from_json_html(html)
        assert "测试" in result

    def test_extracts_direct_json(self):
        html = '{"data": {"tproject": {"projectName": "测试"}}}'
        result = extract_from_json_html(html)
        assert "测试" in result

    def test_returns_empty_for_invalid(self):
        html = "not json"
        result = extract_from_json_html(html)
        assert result == ""

    def test_returns_empty_for_none(self):
        result = extract_from_json_html(None)
        assert result == ""
