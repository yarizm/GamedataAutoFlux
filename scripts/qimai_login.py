"""
辅助脚本：手动登录七麦以获取持久化会话凭证。
运行此脚本会弹出一个浏览器窗口，请您手动完成七麦的登录。
登录完成后，关闭浏览器即可。系统会自动将登录凭证保存在 data/qimai_profile 目录中。
"""
import os
from playwright.sync_api import sync_playwright

def main():
    profile_dir = os.path.join(os.getcwd(), "data", "qimai_profile")
    print(f"正在启动浏览器，配置目录: {profile_dir}")
    print("请在弹出的浏览器中手动登录七麦 (qimai.cn)...")
    
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=profile_dir,
            headless=False,  # 必须非无头模式，方便扫码或密码登录
            viewport={"width": 1280, "height": 720},
            args=["--disable-blink-features=AutomationControlled"],
            ignore_default_args=["--enable-automation"],
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        )
        page = context.new_page()
        
        # 注入反检测脚本隐藏 webdriver 属性
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            window.navigator.chrome = {
                runtime: {},
            };
        """)
        
        page.goto("https://www.qimai.cn")
        
        input("==> 登录完成后，请在此按回车键结束（浏览器会自动关闭）...")
        context.close()
        
    print(f"会话已保存到: {profile_dir}，您可以开始进行 Qimai 数据采集了！")

if __name__ == "__main__":
    main()
