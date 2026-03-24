# -*- coding: utf-8 -*-
"""文本处理工具模块"""
from __future__ import annotations

import html as html_module
import json
import re
from typing import Any, Final, Optional

# 正则表达式模式（编译一次，重复使用）
HTML_TAG_PATTERN: Final[re.Pattern] = re.compile(r"<[^>]+>")
WHITESPACE_PATTERN: Final[re.Pattern] = re.compile(r"\s+")
CJK_PATTERN: Final[re.Pattern] = re.compile(r"[\u4e00-\u9fff]")
DIGIT_PATTERN: Final[re.Pattern] = re.compile(r"\d")
ALPHA_PATTERN: Final[re.Pattern] = re.compile(r"[A-Za-z]")


def strip_html_tags(text: str | None) -> str:
    """去除HTML标签"""
    if not text:
        return ""
    return HTML_TAG_PATTERN.sub(" ", text)


def normalize_whitespace(text: str) -> str:
    """规范化空白字符"""
    return WHITESPACE_PATTERN.sub(" ", text).strip()


def strip_tags_text(text: str | None) -> str:
    """去除HTML标签并规范化空白"""
    if not text:
        return ""
    return normalize_whitespace(strip_html_tags(text))


def strip_tags_preview(text: str | None, max_len: int = 100) -> str:
    """去除HTML标签并截取预览文本"""
    cleaned = strip_tags_text(text)
    if len(cleaned) > max_len:
        return cleaned[:max_len] + "…"
    return cleaned


def count_text_stats(text: str | None) -> dict[str, int]:
    """统计文本中的中文、数字、字母数量"""
    plain = strip_tags_text(text)
    return {
        "cn": len(CJK_PATTERN.findall(plain)),
        "digit": len(DIGIT_PATTERN.findall(plain)),
        "alpha": len(ALPHA_PATTERN.findall(plain)),
        "total": len(CJK_PATTERN.findall(plain))
        + len(DIGIT_PATTERN.findall(plain))
        + len(ALPHA_PATTERN.findall(plain)),
    }


def valid_text_cjk_digit_alpha(text: str | None) -> int:
    """计算有效字符数（中文+数字+字母）"""
    stats = count_text_stats(text)
    return stats["total"]


def short_url(url: str | None, max_len: int = 42) -> str:
    """缩短URL显示"""
    if not url:
        return "—"
    url_str = str(url).strip()
    if len(url_str) > max_len:
        return url_str[:max_len] + "…"
    return url_str


def parse_meta(raw: str | None) -> dict[str, Any]:
    """解析JSON元数据"""
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


def format_meta_for_display(
    meta: dict[str, Any], priority_keys: tuple[str, ...], max_keys: int = 8
) -> list[str]:
    """根据优先级获取要显示的键"""
    all_keys = set(meta.keys())
    ordered = [k for k in priority_keys if k in all_keys]
    rest = sorted(all_keys - set(ordered))
    return ordered + rest[: max_keys - len(ordered)]


def truncate_text(text: str | None, max_len: int = 56) -> str:
    """截断文本"""
    if not text:
        return ""
    text_str = str(text)
    if len(text_str) > max_len:
        return text_str[:max_len] + "…"
    return text_str


def try_parse_json_loose(text: str | None) -> Optional[Any]:
    """从文本中尽量解析出JSON对象"""
    if not text:
        return None

    s = text.strip()
    if not s or s[0] not in "{[":
        return None

    s = html_module.unescape(s)

    try:
        return json.loads(s)
    except json.JSONDecodeError:
        pass

    # 尝试提取JSON对象
    start = s.find("{")
    end = s.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(s[start : end + 1])
        except json.JSONDecodeError:
            pass

    return None


def escape_html(s: Any) -> str:
    """转义HTML特殊字符"""
    if s is None:
        return ""
    text = str(s).strip()
    if not text or text.lower() in ("null", "none"):
        return ""
    return html_module.escape(text).replace("\n", "<br/>\n")
