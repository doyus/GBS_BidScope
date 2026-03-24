# -*- coding: utf-8 -*-
"""
诊断：为何爬虫提示「zhipuai 不可用」。

常见原因：
  1. 运行爬虫的 Python 与 pip 安装目标不一致（例如 PyCharm 用 venv，命令行用系统 python）。
  2. zhipuai 已装但其依赖（httpx/pydantic 等）版本冲突，导入时抛错而非 ModuleNotFoundError。

用法（务必用「和爬虫同一解释器」）：
  python test_zhipuai_env.py
  python test_zhipuai_env.py --ping    # 需环境变量 ZHIPUAI_API_KEY
"""
from __future__ import annotations

import argparse
import importlib.util
import os
import subprocess
import sys


def main() -> None:
    ap = argparse.ArgumentParser(description="zhipuai 环境与导入诊断")
    ap.add_argument(
        "--ping",
        action="store_true",
        help="用 ZHIPUAI_API_KEY 发一条最小 chat 请求（耗额度）",
    )
    args = ap.parse_args()

    base = os.path.dirname(os.path.abspath(__file__))
    venv_py = os.path.join(base, ".venv", "Scripts", "python.exe")
    if os.name != "nt":
        venv_py = os.path.join(base, ".venv", "bin", "python")
    if os.path.isfile(venv_py) and os.path.normcase(sys.executable) != os.path.normcase(
        os.path.abspath(venv_py)
    ):
        print("提示：项目下有 .venv，若包装在 venv 里，请用:")
        print(" ", venv_py, os.path.basename(__file__))
        print()

    print("=" * 60)
    print("1) 当前解释器（爬虫必须用同一个）")
    print("   executable:", sys.executable)
    print("   version:   ", sys.version.split()[0])
    print()

    print("2) pip 认为 zhipuai 装在哪（可能与上面解释器不一致）")
    try:
        r = subprocess.run(
            [sys.executable, "-m", "pip", "show", "zhipuai"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if r.returncode == 0:
            for line in (r.stdout or "").strip().splitlines():
                if line.startswith(("Name:", "Version:", "Location:")):
                    print("  ", line)
        else:
            print("   （此解释器下 pip show zhipuai 失败，可能未安装）")
            print("   stderr:", (r.stderr or "")[:500])
    except Exception as e:
        print("   调用 pip 失败:", e)
    print()

    print("3) 模块路径探测")
    spec = importlib.util.find_spec("zhipuai")
    if spec is None:
        print("   find_spec('zhipuai') -> None（当前解释器找不到包）")
        print("   修复: ", sys.executable, "-m pip install zhipuai")
    else:
        print("   origin:", getattr(spec, "origin", None))
        print("   submodule_search_locations:", spec.submodule_search_locations)
    print()

    print("4) 与爬虫相同：from zhipuai import ZhipuAI")
    try:
        from zhipuai import ZhipuAI

        print("   OK，ZhipuAI =", ZhipuAI)
    except Exception as e:
        print("   失败:", type(e).__name__, ":", e)
        import traceback

        traceback.print_exc()
        print()
        print("若这里失败但 pip 显示已安装，多为依赖冲突，可尝试：")
        print(" ", sys.executable, "-m pip install -U zhipuai httpx pydantic")
        sys.exit(1)
    print()

    if args.ping:
        key = os.environ.get("ZHIPUAI_API_KEY", "").strip()
        if not key:
            print("未设置 ZHIPUAI_API_KEY，跳过 --ping")
            sys.exit(1)
        print("5) API 连通性（--ping）")
        try:
            client = ZhipuAI(api_key=key)
            resp = client.chat.completions.create(
                model=os.environ.get("ZHIPU_MODEL", "glm-4-flash"),
                messages=[{"role": "user", "content": "回复一个字：好"}],
                max_tokens=8,
            )
            text = ""
            if resp.choices and resp.choices[0].message:
                text = (resp.choices[0].message.content or "").strip()
            print("   回复片段:", text[:80])
            print("   OK")
        except Exception as e:
            print("   请求失败:", type(e).__name__, ":", e)
            sys.exit(1)

    print("=" * 60)
    print(
        "结论：本解释器可正常导入 zhipuai。请用上述 executable 运行 crawl_update_content.py。"
    )


if __name__ == "__main__":
    main()
