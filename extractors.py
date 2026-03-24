# -*- coding: utf-8 -*-
"""
内容提取模块 - 分离内容提取逻辑
"""
from __future__ import annotations

import html as html_module
import json
import re
from typing import Any, Optional

from lxml import etree
from lxml import html as lxml_html
from readability import Document

from config import config
from utils import count_valid_text


def is_good_main_content(html_fragment: Optional[str]) -> bool:
    """判断是否为有效的主要内容"""
    if not html_fragment or len(html_fragment) < 80:
        return False

    valid_count = count_valid_text(html_fragment)
    if valid_count < config.crawl.min_text_length:
        return False

    link_ratio = html_fragment.lower().count("<a ") / max(valid_count / 80, 1)
    if link_ratio > 8:
        return False

    return True


def extract_with_readability(html: str) -> str:
    """使用readability提取正文"""
    try:
        doc = Document(html)
        summary = doc.summary()
        if summary and len(summary.strip()) > 50:
            return summary
    except Exception:
        pass
    return ""


def extract_with_trafilatura(html: str, page_url: str = "") -> str:
    """使用trafilatura提取正文"""
    try:
        import trafilatura

        xml_content = trafilatura.extract(
            html,
            url=page_url or None,
            output_format="xml",
            include_links=True,
            include_tables=True,
            include_images=False,
            favor_precision=True,
        )

        if xml_content and len(xml_content.strip()) > 100:
            return f'<div class="trafilatura-extract">{xml_content}</div>'

        text = trafilatura.extract(
            html, url=page_url or None, favor_precision=True, include_tables=True
        )

        if text and count_valid_text(text) >= config.crawl.min_text_length:
            escaped = html_module.escape(text)
            return f'<article class="trafilatura-text"><pre>{escaped}</pre></article>'
    except Exception:
        pass
    return ""


def extract_by_xpaths(html: str, xpaths: Optional[list[str]] = None) -> str:
    """使用XPath列表提取正文"""
    if xpaths is None:
        xpaths = list(config.xpath.static_xpaths)

    try:
        tree = lxml_html.fromstring(html)
    except Exception:
        return ""

    for xpath_expr in xpaths:
        try:
            nodes = tree.xpath(xpath_expr)
            for node in nodes:
                if not hasattr(node, "tag"):
                    continue
                fragment = etree.tostring(node, encoding="unicode", method="html")
                if is_good_main_content(fragment):
                    return fragment
        except Exception:
            continue
    return ""


def extract_largest_text_div(html: str) -> str:
    """提取文本量最大的div（启发式方法）"""
    try:
        tree = lxml_html.fromstring(html)
    except Exception:
        return ""

    body = tree.find(".//body")
    if body is None:
        return ""

    best_html = ""
    best_score = 0.0

    for div in body.iter("div"):
        try:
            text = "".join(div.itertext() or [])
            if count_valid_text(text) < config.crawl.min_valid_loose:
                continue

            link_count = len(div.xpath(".//a"))
            depth = len(list(div.iterancestors()))
            score = len(text) - 15 * min(link_count, 50) - 2 * max(0, depth - 15)

            if score > best_score:
                best_score = score
                best_html = etree.tostring(div, encoding="unicode", method="html")
        except Exception:
            continue

    return best_html if is_good_main_content(best_html) else ""


def apply_single_xpath(html: str, xpath: str) -> str:
    """应用单个XPath提取内容"""
    xpath = (xpath or "").strip()
    if not xpath:
        return ""

    try:
        tree = lxml_html.fromstring(html)
        nodes = tree.xpath(xpath)
        if not nodes:
            return ""

        node = nodes[0]
        if not hasattr(node, "tag"):
            return str(node) if node else ""
        return etree.tostring(node, encoding="unicode", method="html")
    except Exception:
        return ""


def try_parse_json_loose(text: str) -> Optional[Any]:
    """从文本中尝试解析JSON"""
    text = (text or "").strip()
    if not text or text[0] not in "{[":
        return None

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            pass
    return None


def extract_pre_wrapped_json(html: str) -> str:
    """提取<pre>标签包裹的JSON并转换为HTML"""
    if not html or len(html) < 80:
        return ""

    best_data: Optional[Any] = None
    best_len = 0

    for match in re.finditer(r"<pre[^>]*>([\s\S]*?)</pre>", html, re.IGNORECASE):
        raw = match.group(1)
        raw = re.sub(r"<[^>]+>", " ", raw).strip()

        if len(raw) < 50:
            continue

        data = try_parse_json_loose(raw)
        if data is not None and len(raw) > best_len:
            best_len = len(raw)
            best_data = data

    if best_data is None:
        return ""

    article = bidding_json_to_article_html(best_data)
    if article and count_valid_text(article) >= 12:
        return article
    return ""


def bidding_json_to_article_html(data: Any) -> str:
    """将招投标JSON数据转换为可读HTML"""
    if not isinstance(data, dict):
        return ""

    inner = data.get("data")
    if not isinstance(inner, dict):
        inner = data

    project = inner.get("tproject")
    if not isinstance(project, dict):
        project = {}

    parts: list[str] = []

    project_name = project.get("projectName") or inner.get("projectName")
    if project_name:
        escaped_name = html_module.escape(str(project_name).strip())
        parts.append(f'<h1 class="json-project-title">{escaped_name}</h1>')

    blocks = _build_info_blocks(project, inner)
    parts.extend(blocks)

    process_list = inner.get("processList")
    if isinstance(process_list, list) and process_list:
        parts.append(_build_process_list(process_list))

    file_sections = _build_file_sections(inner)
    parts.extend(file_sections)

    extra_rows = _build_extra_rows(project)
    if extra_rows:
        parts.append(
            f'<section class="json-field json-extra"><h2>其他信息</h2>'
            f"<table>{extra_rows}</table></section>"
        )

    if not parts:
        return ""

    return '<article class="from-api-json-detail">' + "\n".join(parts) + "</article>"


def _build_info_blocks(project: dict, inner: dict) -> list[str]:
    """构建信息块"""

    def esc(val: Any) -> str:
        if val is None:
            return ""
        s = str(val).strip()
        if not s or s.lower() in ("null", "none"):
            return ""
        return html_module.escape(s).replace("\n", "<br/>\n")

    def section(title: str, body: str) -> str:
        if not body:
            return ""
        return (
            f'<section class="json-field"><h2>{html_module.escape(title)}</h2>'
            f"<div>{body}</div></section>"
        )

    parts: list[str] = []

    low_cap = project.get("lowCapital")
    high_cap = project.get("highCapital")
    cap_text = _format_capital_range(low_cap, high_cap)

    info_items = [
        ("项目编号", project.get("projectNo")),
        ("项目分类", inner.get("projectClassName")),
        ("采购/项目说明", project.get("projectMessage")),
        ("项目地址", project.get("projectAddress")),
        ("采购部门", project.get("purchaseDept")),
        ("联系人", _format_contact(project)),
        ("项目负责人", project.get("projectManager")),
        ("预算", cap_text or None),
        ("报价起止", _format_bid_time(project)),
        ("项目内容 / 采购需求", project.get("projectContent")),
        ("资质要求", project.get("qualificationRequier")),
        ("备注", project.get("projectRemarks")),
    ]

    for title, value in info_items:
        escaped = esc(value)
        if escaped:
            parts.append(section(title, escaped))

    return parts


def _format_capital_range(low: Any, high: Any) -> Optional[str]:
    """格式化预算范围"""
    try:
        if low is not None and high is not None:
            lo, hi = float(low), float(high)
            if lo or hi:
                if lo != hi:
                    return f"{lo:g} ～ {hi:g} 万元"
                return f"{hi:g} 万元"
    except (TypeError, ValueError):
        pass
    return None


def _format_contact(project: dict) -> str:
    """格式化联系人信息"""
    name = project.get("purchaserName")
    mobile = project.get("mobile")
    parts = [str(x) for x in (name, mobile) if x and str(x).strip()]
    return " ".join(parts) if parts else ""


def _format_bid_time(project: dict) -> str:
    """格式化报价时间"""
    start = project.get("projectBjKssj")
    end = project.get("projectBjJssj")
    parts = [str(x) for x in (start, end) if x and str(x).strip()]
    return " ~ ".join(parts) if parts else ""


def _build_process_list(process_list: list) -> str:
    """构建流程列表HTML"""
    items: list[str] = []

    for process in process_list:
        if not isinstance(process, dict):
            continue

        name = html_module.escape(str(process.get("processName", "")).strip())
        create_time = html_module.escape(str(process.get("createTime", "")).strip())

        if name:
            time_span = f" <span>{create_time}</span>" if create_time else ""
            items.append(f"<li>{name}{time_span}</li>")

    if not items:
        return ""

    return (
        '<section class="json-field"><h2>流程节点</h2><ul>'
        + "".join(items)
        + "</ul></section>"
    )


def _build_file_sections(inner: dict) -> list[str]:
    """构建文件区块"""

    def section(title: str, obj: Any) -> str:
        if obj is None:
            return ""

        if isinstance(obj, dict):
            name = obj.get("fileName") or obj.get("name")
            if name:
                escaped = html_module.escape(str(name).strip())
                return (
                    f'<section class="json-field"><h2>{html_module.escape(title)}</h2>'
                    f"<div>{escaped}</div></section>"
                )
        elif isinstance(obj, list):
            names = []
            for item in obj:
                if isinstance(item, dict) and item.get("fileName"):
                    names.append(html_module.escape(str(item["fileName"]).strip()))
            if names:
                items = "".join(f"<li>{n}</li>" for n in names)
                return (
                    f'<section class="json-field"><h2>{html_module.escape(title)}</h2>'
                    f"<ul>{items}</ul></section>"
                )
        return ""

    return [
        section("采购需求文件", inner.get("cgxqFile")),
        section("附件清单", inner.get("fjclFile")),
    ]


def _build_extra_rows(project: dict) -> str:
    """构建额外信息行"""
    shown_keys = {
        "projectName",
        "projectNo",
        "projectMessage",
        "projectAddress",
        "purchaseDept",
        "purchaserName",
        "mobile",
        "projectManager",
        "projectContent",
        "qualificationRequier",
        "projectRemarks",
        "lowCapital",
        "highCapital",
        "projectBjKssj",
        "projectBjJssj",
    }

    extra: list[tuple[str, str]] = []
    for key, value in sorted(project.items(), key=lambda x: str(x[0])):
        if key in shown_keys or value is None:
            continue

        if isinstance(value, str) and len(value.strip()) > 2:
            if count_valid_text(value) >= 4 or len(value) > 30:
                extra.append((str(key), value.strip()))

    if not extra:
        return ""

    rows = "".join(
        f"<tr><th>{html_module.escape(k)}</th><td>{html_module.escape(v)}</td></tr>"
        for k, v in extra[:25]
    )
    return rows


def try_fetch_json_url(url: str) -> tuple[Optional[Any], str, int]:
    """尝试从URL获取JSON数据"""
    try:
        import requests

        requests.packages.urllib3.disable_warnings()
    except ImportError:
        return None, "", 0

    try:
        response = requests.get(
            url, headers=config.url.json_fetch_headers, timeout=40, allow_redirects=True
        )
        status_code = response.status_code

        if status_code in (404, 410):
            return None, "", status_code

        response.raise_for_status()
        text = (response.text or "").lstrip("\ufeff").strip()

        if not text or text[0] not in "{[":
            return None, "", status_code

        data = json.loads(text)
        sample = (
            text
            if len(text) <= config.zhipu.json_max_chars
            else text[: config.zhipu.json_max_chars]
        )
        return data, sample, status_code

    except Exception as e:
        status_code = 0
        resp = getattr(e, "response", None)
        if resp is not None:
            try:
                status_code = int(getattr(resp, "status_code", 0) or 0)
            except (TypeError, ValueError):
                status_code = 0

        if status_code in (404, 410):
            return None, "", status_code
        return None, "", status_code


def json_path_get(root: Any, path: str) -> Any:
    """从JSON对象按路径获取值"""
    path = (path or "").strip().strip("$").strip(".")
    if not path:
        return root

    current = root
    for part in re.split(r"\.|\[|\]", path):
        if not part:
            continue

        if part.isdigit():
            idx = int(part)
            if isinstance(current, list) and 0 <= idx < len(current):
                current = current[idx]
            else:
                return None
        else:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None

    return current
