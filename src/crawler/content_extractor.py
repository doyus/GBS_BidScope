# -*- coding: utf-8 -*-
"""内容提取器模块"""
from __future__ import annotations

import html as html_module
import json
from typing import Any, Final, Optional

from readability import Document

from src.config import settings
from src.utils.text_utils import (
    count_text_stats,
    escape_html,
    strip_tags_text,
    try_parse_json_loose,
    valid_text_cjk_digit_alpha,
)

# 内容质量阈值
MIN_TEXT_LEN: Final[int] = settings.crawl.min_text_len
MIN_CJK_ARTICLE: Final[int] = settings.crawl.min_cjk_article


def is_good_main_content(html_fragment: str | None) -> bool:
    """判断内容质量是否合格"""
    if not html_fragment or len(html_fragment) < 80:
        return False

    vc = valid_text_cjk_digit_alpha(html_fragment)
    if vc < MIN_TEXT_LEN:
        return False

    # 检查链接比例（避免导航页）
    link_ratio = html_fragment.lower().count("<a ") / max(vc / 80, 1)
    if link_ratio > 8:
        return False

    return True


def extract_readability(html: str | None) -> str:
    """使用readability提取正文"""
    if not html:
        return ""

    try:
        doc = Document(html)
        summary = doc.summary()
        if summary and len(summary.strip()) > 50:
            return summary
    except Exception:
        pass

    return ""


def extract_trafilatura(html: str | None, page_url: str | None = None) -> str:
    """使用trafilatura提取正文"""
    if not html:
        return ""

    try:
        import trafilatura

        # 尝试XML格式输出
        xml = trafilatura.extract(
            html,
            url=page_url or None,
            output_format="xml",
            include_links=True,
            include_tables=True,
            include_images=False,
            favor_precision=True,
        )
        if xml and len(xml.strip()) > 100:
            return f'<div class="trafilatura-extract">{xml}</div>'

        # 尝试纯文本输出
        text = trafilatura.extract(
            html,
            url=page_url or None,
            favor_precision=True,
            include_tables=True,
        )
        if text and valid_text_cjk_digit_alpha(text) >= MIN_TEXT_LEN:
            escaped = html_module.escape(text)
            return f'<article class="trafilatura-text"><pre>{escaped}</pre></article>'
    except Exception:
        pass

    return ""


def extract_heuristic(html: str | None) -> str:
    """启发式提取正文"""
    if not html:
        return ""

    from lxml import html as lxml_html

    try:
        tree = lxml_html.fromstring(html)

        # 常见内容选择器
        selectors = [
            "//article",
            "//main",
            "//div[@class='content']",
            "//div[@id='content']",
            "//div[@class='detail']",
            "//div[@class='article']",
            "//div[@class='main-content']",
        ]

        for selector in selectors:
            elements = tree.xpath(selector)
            if elements:
                content = lxml_html.tostring(elements[0], encoding="unicode")
                if valid_text_cjk_digit_alpha(content) >= MIN_TEXT_LEN:
                    return content
    except Exception:
        pass

    return ""


def extract_all_methods(html: str | None, page_url: str | None = None) -> str:
    """尝试所有方法提取正文，返回最佳结果"""
    if not html:
        return ""

    # 按优先级尝试各种方法
    methods = [
        ("readability", lambda: extract_readability(html)),
        ("trafilatura", lambda: extract_trafilatura(html, page_url)),
        ("heuristic", lambda: extract_heuristic(html)),
    ]

    best_content = ""
    best_score = 0

    for name, method in methods:
        try:
            content = method()
            if content:
                score = valid_text_cjk_digit_alpha(content)
                if score > best_score:
                    best_score = score
                    best_content = content
        except Exception:
            continue

    return best_content


def _bidding_json_to_article_html(data: Any) -> str:
    """将招投标JSON转换为HTML文章"""
    if not isinstance(data, dict):
        return ""

    inner = data.get("data")
    if not isinstance(inner, dict):
        inner = data

    tp = inner.get("tproject")
    if not isinstance(tp, dict):
        tp = {}

    def section(title: str, body: str) -> str:
        if not body:
            return ""
        return (
            f'<section class="json-field"><h2>{html_module.escape(title)}</h2>'
            f"<div>{body}</div></section>"
        )

    parts: list[str] = []

    # 项目标题
    pname = tp.get("projectName") or inner.get("projectName")
    if pname:
        parts.append(f'<h1 class="json-project-title">{escape_html(pname)}</h1>')

    # 预算信息
    low_c = tp.get("lowCapital")
    high_c = tp.get("highCapital")
    cap_txt = ""
    try:
        if low_c is not None and high_c is not None:
            lo, hi = float(low_c), float(high_c)
            if lo or hi:
                cap_txt = f"{lo:g} ～ {hi:g} 万元" if lo != hi else f"{hi:g} 万元"
    except (TypeError, ValueError):
        pass

    # 基本信息块
    blocks = [
        ("项目编号", tp.get("projectNo")),
        ("项目分类", inner.get("projectClassName")),
        ("采购/项目说明", tp.get("projectMessage")),
        ("项目地址", tp.get("projectAddress")),
        ("采购部门", tp.get("purchaseDept")),
        (
            "联系人",
            " ".join(
                x
                for x in (tp.get("purchaserName"), tp.get("mobile"))
                if x and str(x).strip()
            ),
        ),
        ("项目负责人", tp.get("projectManager")),
        ("预算", cap_txt or None),
        (
            "报价起止",
            " ~ ".join(
                x
                for x in (
                    tp.get("projectBjKssj"),
                    tp.get("projectBjJssj"),
                )
                if x and str(x).strip()
            ),
        ),
        ("项目内容 / 采购需求", tp.get("projectContent")),
        ("资质要求", tp.get("qualificationRequier")),
        ("备注", tp.get("projectRemarks")),
    ]

    for title, val in blocks:
        escaped = escape_html(val)
        if escaped:
            parts.append(section(title, escaped))

    # 流程节点
    pl = inner.get("processList")
    if isinstance(pl, list) and pl:
        lis = []
        for p in pl:
            if not isinstance(p, dict):
                continue
            nm = escape_html(p.get("processName"))
            ct = escape_html(p.get("createTime"))
            if nm:
                lis.append(
                    f"<li>{nm}" + (f" <span>{ct}</span>" if ct else "") + "</li>"
                )
        if lis:
            parts.append(
                '<section class="json-field"><h2>流程节点</h2><ul>'
                + "".join(lis)
                + "</ul></section>"
            )

    return "".join(parts)


def extract_from_json_html(html: str | None) -> str:
    """从JSON格式的HTML中提取内容"""
    if not html:
        return ""

    # 检查是否是<pre>包裹的JSON
    if "<pre>" in html and "</pre>" in html:
        try:
            import re

            match = re.search(r"<pre[^>]*>(.*?)</pre>", html, re.DOTALL)
            if match:
                json_text = match.group(1)
                data = try_parse_json_loose(json_text)
                if data:
                    return _bidding_json_to_article_html(data)
        except Exception:
            pass

    # 尝试直接解析JSON
    data = try_parse_json_loose(html)
    if data:
        return _bidding_json_to_article_html(data)

    return ""
