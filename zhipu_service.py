# -*- coding: utf-8 -*-
"""
智谱AI服务模块 - 提供AI内容提取功能
"""
from __future__ import annotations

import json
import traceback
from typing import Any, Optional

from zhipuai import ZhipuAI

from config import config


class ZhipuService:
    """智谱AI服务类"""

    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        self._api_key = api_key or config.zhipu.api_key
        self._model = model or config.zhipu.model

        if not self._api_key:
            raise ValueError("请设置环境变量 ZHIPU_API_KEY 或创建 .env 文件")

        self._client = ZhipuAI(api_key=self._api_key)

    def extract_location_info(
        self, title: str, content: str, max_tokens: int = 100
    ) -> list[dict[str, str]]:
        """从招投标公告中提取地区信息"""
        prompt = self._build_location_prompt(title, content)

        try:
            response = self._client.chat.completions.create(
                model=self._model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                max_tokens=max_tokens,
            )

            return self._parse_location_response(response)
        except Exception as e:
            print(f"API调用失败: {str(e)}")
            traceback.print_exc()
            return []

    def _build_location_prompt(self, title: str, content: str) -> str:
        """构建地区提取提示词"""
        return (
            f"请从所给的招投标公告中提取出，招标人的所处的县,市,省 这三个字段,"
            f"对应字段没有的写成未找到，以JSON数组形式返回，"
            f"输出时候要把地区格式化一下，不能有的是山西，有的是山西省，"
            f"而且只有县市有值，省肯定有值的，自己推一下："
            f"标题：{title}, 内容：{content}"
        )

    def _parse_location_response(self, response: Any) -> list[dict[str, str]]:
        """解析地区提取响应"""
        try:
            content = response.choices[0].message.content
            print(content)

            result = json.loads(content)

            if isinstance(result, list):
                return result

            if isinstance(result, dict):
                if any(key in result for key in ("省", "市", "县")):
                    return [result]

                for value in result.values():
                    if isinstance(value, list) and value and isinstance(value[0], dict):
                        return value

                return [result]

            return [{}]
        except (json.JSONDecodeError, KeyError, IndexError):
            return [{}]

    def extract_content_xpath(self, html: str, sample_url: str = "") -> Optional[str]:
        """使用AI从HTML中提取正文XPath"""
        if not html:
            return None

        truncated_html = html[: config.zhipu.html_max_chars]

        prompt = (
            f"请分析以下HTML内容，找出包含主要正文内容的元素的XPath表达式。"
            f"只返回XPath字符串，不要其他解释。\n"
            f"URL: {sample_url}\n"
            f"HTML: {truncated_html}"
        )

        try:
            response = self._client.chat.completions.create(
                model=self._model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=200,
            )

            xpath = response.choices[0].message.content.strip()
            if xpath.startswith("//") or xpath.startswith("/"):
                return xpath
            return None
        except Exception as e:
            print(f"XPath提取失败: {str(e)}")
            return None

    def reorganize_content(self, raw_text: str, context: str = "") -> str:
        """使用AI重组和清理内容"""
        if not raw_text:
            return ""

        prompt = (
            f"请将以下内容重新组织成结构清晰的文本，"
            f"去除无关信息，保留核心内容：\n"
            f"{context}\n{raw_text[:config.zhipu.html_max_chars]}"
        )

        try:
            response = self._client.chat.completions.create(
                model=self._model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=2000,
            )

            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"内容重组失败: {str(e)}")
            return raw_text


_service_instance: Optional[ZhipuService] = None


def get_zhipu_service() -> ZhipuService:
    """获取智谱AI服务单例"""
    global _service_instance
    if _service_instance is None:
        _service_instance = ZhipuService()
    return _service_instance


def extract_location(title: str, content: str) -> list[dict[str, str]]:
    """提取地区信息（便捷函数）"""
    return get_zhipu_service().extract_location_info(title, content)


if __name__ == "__main__":
    service = ZhipuService()
    result = service.extract_location_info(
        "上海市徐汇区人民政府龙华街道办事处框架协议项目",
        "相关标段 收起 标段(包)名称 标段(包) 2026-03-11 12:34:26",
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
