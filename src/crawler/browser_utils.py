# -*- coding: utf-8 -*-
"""浏览器工具模块"""
from __future__ import annotations

import time
from typing import Any, Final, Optional

from src.config import settings

# 浏览器异常触发词
BROWSER_ERROR_TRIGGERS: Final[tuple[str, ...]] = settings.BROWSER_ERROR_TRIGGERS


def is_browser_disconnected_error(error: BaseException) -> bool:
    """检查是否是浏览器断开连接错误"""
    if type(error).__name__ == "PageDisconnectedError":
        return True

    try:
        from DrissionPage.errors import PageDisconnectedError as PDE

        return isinstance(error, PDE)
    except ImportError:
        pass

    msg = str(error).lower()
    return any(trigger in msg for trigger in BROWSER_ERROR_TRIGGERS)


def should_restart_browser(error: BaseException) -> bool:
    """判断是否应该重启浏览器"""
    msg = str(error).lower()
    return any(trigger in msg for trigger in BROWSER_ERROR_TRIGGERS)


def apply_auto_accept_dialogs(page: Any) -> None:
    """设置自动接受浏览器对话框"""
    try:
        from DrissionPage._functions.settings import Settings

        Settings.set_auto_handle_alert(True)
    except Exception:
        pass

    try:
        page.set.auto_handle_alert(True, accept=True)
    except Exception:
        pass

    try:
        if hasattr(page, "browser") and hasattr(page.browser, "set"):
            page.browser.set.auto_handle_alert(True, accept=True)
    except Exception:
        pass


def restart_browser_page(page: Any, co: Any) -> Any:
    """重启浏览器页面"""
    try:
        page.quit()
    except Exception:
        pass

    time.sleep(0.5)

    from DrissionPage import ChromiumPage

    new_page = ChromiumPage(addr_or_opts=co)
    apply_auto_accept_dialogs(new_page)
    return new_page


def reconnect_browser_if_needed(page: Any, co: Any, error: BaseException) -> Any:
    """如果需要则重新连接浏览器"""
    if not should_restart_browser(error):
        return page

    try:
        page.quit()
    except Exception:
        pass

    try:
        from DrissionPage import ChromiumPage

        new_page = ChromiumPage(addr_or_opts=co)
        apply_auto_accept_dialogs(new_page)
        return new_page
    except Exception as e2:
        return page


def scroll_until_stable(
    page: Any,
    pause: float = 0.55,
    max_rounds: int = 40,
    stable_need: int = 4,
) -> None:
    """滚动页面直到高度稳定"""
    last_height: Optional[float] = None
    stable_count = 0

    inner_js = """
    (function(){
      document.querySelectorAll('*').forEach(function(el){
        try {
          var sh = el.scrollHeight, ch = el.clientHeight;
          if (sh > ch + 100 && el.scrollTop + ch < sh - 20) {
            el.scrollTop = sh;
          }
        } catch(e) {}
      });
    })();
    """

    for _ in range(max_rounds):
        # 滚动到底部
        try:
            page.scroll.to_bottom()
        except Exception:
            try:
                page.run_js(
                    "window.scrollTo(0, Math.max(document.body.scrollHeight,"
                    "document.documentElement.scrollHeight));"
                )
            except Exception:
                break

        time.sleep(pause)

        # 触发内部滚动容器
        try:
            page.run_js(inner_js)
        except Exception:
            pass

        time.sleep(max(0.15, pause * 0.35))

        # 获取当前高度
        try:
            cur = page.run_js(
                "return Math.max(document.documentElement.scrollHeight||0,"
                "document.body.scrollHeight||0);"
            )
            cur = float(cur) if cur else 0.0
        except Exception:
            cur = 0.0

        # 检查是否稳定
        if last_height is not None and abs(cur - last_height) < 2:
            stable_count += 1
            if stable_count >= stable_need:
                break
        else:
            stable_count = 0

        last_height = cur

    # 最终滚动
    try:
        page.scroll.to_bottom()
        time.sleep(min(1.0, pause + 0.2))
        page.run_js(inner_js)
        time.sleep(0.25)
    except Exception:
        pass
