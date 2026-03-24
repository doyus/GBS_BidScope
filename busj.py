import requests
from readability import Document

url = 'http://www.ygcgfw.com/gggs/001001/001001003/20260318/03ecdd9b-2fdb-4d70-8d1f-c0d68925ce76.html'

# 1. 添加请求头，模拟浏览器，防止被反爬
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
}

response = requests.get(url, headers=headers)

# 2. 【关键修改】使用 .text 而不是 .content
# 如果自动识别编码有误（出现乱码），可以手动指定：response.encoding = 'utf-8' 或 'gbk'
html_str = response.text

# 可选：打印确认类型，应该是 <class 'str'>
# print(type(html_str))

try:
    doc = Document(html_str)
    print("标题:", doc.title())
    print("-" * 30)
    print("正文预览:")
    print(doc.text_content()[:500]) # 打印前500字
except Exception as e:
    print(f"提取失败: {e}")
    # 调试信息：如果失败，可能是编码问题，尝试手动指定编码
    # response.encoding = 'gbk'
    # doc = Document(response.text)