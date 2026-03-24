# -*- coding: utf-8 -*-
"""
智谱 AI 客户端模块
用于从招投标公告中提取地区信息
"""
from __future__ import annotations

import json
import traceback
from typing import Any, Optional

from zhipuai import ZhipuAI

from config import ZHIPU_API_KEY, ZHIPU_MODEL


# =============================================================================
# 初始化智谱 AI 客户端
# =============================================================================
def init_zhipu_client() -> ZhipuAI:
    """初始化智谱 AI 客户端"""
    if not ZHIPU_API_KEY:
        raise ValueError("请设置环境变量 ZHIPU_API_KEY 或创建 .env 文件")
    return ZhipuAI(api_key=ZHIPU_API_KEY)


# 全局客户端实例
_client: Optional[ZhipuAI] = None


def get_client() -> ZhipuAI:
    """获取或创建智谱 AI 客户端实例（单例模式）"""
    global _client
    if _client is None:
        _client = init_zhipu_client()
    return _client


# =============================================================================
# AI 回答提取函数
# =============================================================================
def _parse_ai_response(content: str) -> list[dict[str, Any]]:
    """解析 AI 返回的 JSON 响应"""
    try:
        res = json.loads(content)
        if isinstance(res, list):
            return res
        if isinstance(res, dict):
            if any(k in res for k in ("省", "市", "县")):
                return [res]
            for v in res.values():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    return v
            return [res]
        return [{}]
    except json.JSONDecodeError:
        traceback.print_exc()
        print(f"JSON 解析失败: {content[:200]}")
        return []


def get_ai_answer(title: str, content: str) -> list[dict[str, Any]]:
    """
    从招投标公告中提取地区信息（省、市、县）

    Args:
        title: 公告标题
        content: 公告内容

    Returns:
        包含地区信息的字典列表，格式为 [{"省": "...", "市": "...", "县": "..."}]
    """
    try:
        client = get_client()
        prompt = (
            f"请从所给的招投标公告中提取出，招标人的所处的县,市,省 这三个字段,"
            f"对应字段没有的写成未找到，以JSON数组形式返回，输出时候要把地区格式化一下，"
            f"不能有的是山西，有的是山西省，而且只有县市有值，省肯定有值的，自己推一下："
            f"标题：{title}, 内容：{content}"
        )

        response = client.chat.completions.create(
            model=ZHIPU_MODEL,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            max_tokens=100,
        )

        rep_res = response.choices[0].message.content
        print(f"AI 响应: {rep_res}")
        return _parse_ai_response(rep_res)

    except Exception as e:
        traceback.print_exc()
        print(f"API 调用失败: {str(e)}")
        return []


# =============================================================================
# JSON 和 XPath 提取相关功能
# =============================================================================
def ask_llm_json_html_path(page_url: str, json_snippet: str) -> str:
    """
    大模型返回 JSON 里正文 HTML 的路径，如 data.result.content

    Args:
        page_url: 页面 URL
        json_snippet: JSON 片段

    Returns:
        JSON 路径字符串，如 "data.content"
    """
    try:
        client = get_client()
        prompt = f"""你是接口分析专家。下面是某招投标/政府采购相关接口返回的 JSON（可能已截断）。

接口 URL：{page_url}

请找出：存放「公告正文」的那段 **HTML 字符串** 在 JSON 里的访问路径（从根对象一层层下去）。

只输出一个 JSON 对象，不要 markdown：
{{"json_path":"路径","note":"可选"}}

路径格式要求（须能被程序按段解析）：
- 只用英文键名、数字下标，形如：data.content 或 result.items[0].htmlBody 或 list[0].detail.ggText
- 键之间用点号 . 连接，数组用 [0]、[1]，不要写成 /aa/bb 或 $..xx
- 若没有任何字段是较长的 HTML 字符串，json_path 填空字符串"""

        raw = ""
        from config import LLM_JSON_MAX

        resp = client.chat.completions.create(
            model=ZHIPU_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": prompt
                    + "\n\n--- JSON ---\n"
                    + json_snippet[:LLM_JSON_MAX],
                }
            ],
            response_format={"type": "json_object"},
            max_tokens=400,
        )
        raw = (resp.choices[0].message.content or "").strip()
        data = json.loads(raw)
        if isinstance(data, dict):
            p = data.get("json_path") or data.get("path") or data.get("jsonPath") or ""
            return str(p).strip()
    except Exception:
        traceback.print_exc()
        print(f"智谱 json_path 解析失败，原始响应: {raw[:200]}")

    import re

    m = re.search(r'"json_path"\s*:\s*"((?:[^"\\]|\\.)*)"', raw or "", re.I)
    if m:
        return m.group(1).replace('\\"', '"').strip()
    return ""


def ask_llm_for_xpath(page_url: str, html_snippet: str) -> str:
    """
    让大模型推荐提取正文的 XPath

    Args:
        page_url: 页面 URL
        html_snippet: HTML 片段

    Returns:
        XPath 表达式
    """
    try:
        client = get_client()
        prompt = f"""你是网页正文抽取专家。请分析下面的 HTML 代码片段，推荐一个合适的 XPath 表达式用于提取正文内容。

页面 URL：{page_url}

要求：
1. 返回的 XPath 应该能准确定位到包含主要内容的元素
2. 优先考虑 article、main、content 等语义化标签或类名
3. 避免使用过于脆弱的绝对路径
4. 只返回 JSON 格式，不要其他内容

返回格式示例：
{{"xpath":"//article","confidence":0.9}}"""

        from config import LLM_HTML_MAX

        resp = client.chat.completions.create(
            model=ZHIPU_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": prompt
                    + "\n\n--- HTML ---\n"
                    + html_snippet[:LLM_HTML_MAX],
                }
            ],
            response_format={"type": "json_object"},
            max_tokens=200,
        )

        raw = (resp.choices[0].message.content or "").strip()
        data = json.loads(raw)
        if isinstance(data, dict):
            return data.get("xpath", "")
    except Exception:
        traceback.print_exc()
        print(f"智谱 XPath 推荐失败")

    return ""


if __name__ == "__main__":
    # 测试地区提取功能
    sample_title = "上海市徐汇区人民政府龙华街道办事处框架协议项目"
    sample_content = """
相关标段
收起
标段(包)名称
标段(包)
2026-03-11 12:34:26
    """
    result = get_ai_answer(sample_title, sample_content)
    print(f"提取结果: {json.dumps(result, ensure_ascii=False, indent=2)}")
