# -*- coding: utf-8 -*-
import sys
import os
import time
import traceback

# 尝试加载 .env 文件
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import requests
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
requests.packages.urllib3.disable_warnings()
import pandas as pd
from zhipuai import ZhipuAI
import json

name_count = {}
company_list = []
result = []
from datetime import datetime

# 从环境变量读取API Key
ZHIPU_API_KEY = os.environ.get("ZHIPU_API_KEY", "")
if not ZHIPU_API_KEY:
    raise ValueError("请设置环境变量 ZHIPU_API_KEY 或创建 .env 文件")

client = ZhipuAI(api_key=ZHIPU_API_KEY)

def get_ai_answer(title, content):
    """获取AI答案并处理异常"""
    try:
        response = client.chat.completions.create(
            model="GLM-4-Flash-250414",
            messages=[{
                "role": "user",
                "content": f"请从所给的招投标公告中提取出，招标人的所处的县,市,省 这三个字段,对应字段没有的写成未找到，以JSON数组形式返回，输出时候要把地区格式化一下，不能有的是山西，有的是山西省，而且只有县市有值，省肯定有值的，自己推一下：标题：{title}, 内容：{content}"
                # 3. 限制输入长度
            }],
            response_format={"type": "json_object"},
            max_tokens=100  # 4. 限制输出长度
        )
        rep_res = response.choices[0].message.content
        print(rep_res)
        res = json.loads(rep_res)
        # json_object 时可能是 {"省":...}；若要求数组也可能是 [{"省":...}]
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
    except Exception as e:
        print(traceback.print_exc())
        print(f"API调用失败: {str(e)}")
        return []

if __name__  == "__main__":
    print(get_ai_answer("上海市徐汇区人民政府龙华街道办事处框架协议项目", '''
相关标段
收起
标段(包)名称
标段(包)
2026-03-11 12:34:26
    '''))



