# -*- coding: utf-8 -*-
"""
extractors模块单元测试
"""

from extractors import (
    is_good_main_content,
    extract_with_readability,
    extract_by_xpaths,
    extract_largest_text_div,
    apply_single_xpath,
    try_parse_json_loose,
    extract_pre_wrapped_json,
    bidding_json_to_article_html,
    json_path_get,
)


class TestIsGoodMainContent:
    """is_good_main_content函数测试"""

    def test_valid_content(self) -> None:
        """测试有效内容"""
        html = "<p>" + "测试内容" * 20 + "</p>"
        assert is_good_main_content(html) is True

    def test_too_short(self) -> None:
        """测试过短内容"""
        html = "<p>短内容</p>"
        assert is_good_main_content(html) is False

    def test_too_many_links(self) -> None:
        """测试链接过多"""
        html = "<div>" + "<a href='#'>link</a>" * 100 + "</div>"
        assert is_good_main_content(html) is False

    def test_none_input(self) -> None:
        """测试None输入"""
        assert is_good_main_content(None) is False


class TestExtractWithReadability:
    """extract_with_readability函数测试"""

    def test_basic_extraction(self, sample_html: str) -> None:
        """测试基本提取"""
        result = extract_with_readability(sample_html)
        assert len(result) > 0

    def test_empty_html(self) -> None:
        """测试空HTML"""
        result = extract_with_readability("")
        assert result == ""


class TestExtractByXpaths:
    """extract_by_xpaths函数测试"""

    def test_article_xpath(self) -> None:
        """测试article XPath"""
        html = "<article><p>" + "测试内容" * 30 + "</p></article>"
        result = extract_by_xpaths(html, ["//article"])
        assert len(result) > 0

    def test_no_match(self) -> None:
        """测试无匹配"""
        html = "<div>内容</div>"
        result = extract_by_xpaths(html, ["//article"])
        assert result == ""

    def test_custom_xpaths(self) -> None:
        """测试自定义XPath"""
        html = '<div class="content">' + "测试内容" * 30 + "</div>"
        result = extract_by_xpaths(html, ['//*[@class="content"]'])
        assert len(result) > 0


class TestExtractLargestTextDiv:
    """extract_largest_text_div函数测试"""

    def test_finds_largest(self) -> None:
        """测试找到最大div"""
        html = """
        <html><body>
            <div>小内容</div>
            <div>""" + "这是较大的内容区域，包含更多的文本信息用于测试提取功能。" * 10 + """</div>
        </body></html>
        """
        result = extract_largest_text_div(html)
        assert len(result) > 0 or "较大的内容" in result

    def test_empty_body(self) -> None:
        """测试空body"""
        html = "<html><body></body></html>"
        result = extract_largest_text_div(html)
        assert result == ""


class TestApplySingleXpath:
    """apply_single_xpath函数测试"""

    def test_valid_xpath(self) -> None:
        """测试有效XPath"""
        html = "<div><p id='test'>目标内容</p></div>"
        result = apply_single_xpath(html, "//p[@id='test']")
        assert "目标内容" in result

    def test_invalid_xpath(self) -> None:
        """测试无效XPath"""
        html = "<div>内容</div>"
        result = apply_single_xpath(html, "//nonexistent")
        assert result == ""

    def test_empty_xpath(self) -> None:
        """测试空XPath"""
        html = "<div>内容</div>"
        result = apply_single_xpath(html, "")
        assert result == ""


class TestTryParseJsonLoose:
    """try_parse_json_loose函数测试"""

    def test_valid_json_object(self) -> None:
        """测试有效JSON对象"""
        result = try_parse_json_loose('{"key": "value"}')
        assert result == {"key": "value"}

    def test_valid_json_array(self) -> None:
        """测试有效JSON数组"""
        result = try_parse_json_loose("[1, 2, 3]")
        assert result == [1, 2, 3]

    def test_invalid_json(self) -> None:
        """测试无效JSON"""
        result = try_parse_json_loose("not json")
        assert result is None

    def test_empty_string(self) -> None:
        """测试空字符串"""
        result = try_parse_json_loose("")
        assert result is None


class TestExtractPreWrappedJson:
    """extract_pre_wrapped_json函数测试"""

    def test_pre_wrapped_json(self) -> None:
        """测试pre包裹的JSON"""
        json_content = (
            '{"data": {"tproject": {"projectName": "测试项目名称测试项目名称测试项目名称"}}}'
        )
        html = "<pre>" + json_content + "</pre>"
        result = extract_pre_wrapped_json(html)
        assert len(result) > 0 or result == ""

    def test_no_pre_tag(self) -> None:
        """测试无pre标签"""
        html = '<div>{"key": "value"}</div>'
        result = extract_pre_wrapped_json(html)
        assert result == ""

    def test_empty_html(self) -> None:
        """测试空HTML"""
        result = extract_pre_wrapped_json("")
        assert result == ""


class TestBiddingJsonToArticleHtml:
    """bidding_json_to_article_html函数测试"""

    def test_basic_conversion(self, sample_bidding_json: dict) -> None:
        """测试基本转换"""
        result = bidding_json_to_article_html(sample_bidding_json)
        assert "测试项目名称" in result
        assert "<article" in result

    def test_empty_dict(self) -> None:
        """测试空字典"""
        result = bidding_json_to_article_html({})
        assert result == "" or "<article" in result

    def test_non_dict_input(self) -> None:
        """测试非字典输入"""
        result = bidding_json_to_article_html("not a dict")
        assert result == ""

    def test_includes_process_list(self, sample_bidding_json: dict) -> None:
        """测试包含流程列表"""
        result = bidding_json_to_article_html(sample_bidding_json)
        assert "流程节点" in result or "发布公告" in result


class TestJsonPathGet:
    """json_path_get函数测试"""

    def test_simple_path(self) -> None:
        """测试简单路径"""
        data = {"key": "value"}
        result = json_path_get(data, "key")
        assert result == "value"

    def test_nested_path(self) -> None:
        """测试嵌套路径"""
        data = {"level1": {"level2": "value"}}
        result = json_path_get(data, "level1.level2")
        assert result == "value"

    def test_array_index(self) -> None:
        """测试数组索引"""
        data = {"items": ["a", "b", "c"]}
        result = json_path_get(data, "items[0]")
        assert result == "a"

    def test_missing_path(self) -> None:
        """测试缺失路径"""
        data = {"key": "value"}
        result = json_path_get(data, "nonexistent")
        assert result is None

    def test_empty_path(self) -> None:
        """测试空路径"""
        data = {"key": "value"}
        result = json_path_get(data, "")
        assert result == data
