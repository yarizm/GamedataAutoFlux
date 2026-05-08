"""生成 Steam 游戏列表本地缓存（可选）

Steam 官方 GetAppList 旧接口 (ISteamApps/GetAppList/v2) 已弃用，
新接口为 IStoreService/GetAppList/v1，域名改为 partner.steam-api.com。

**重要**: 此接口属于 Steam Partner API，需要**发行商级别的 API Key**，
普通的 Steam Web API Key（从 steamcommunity.com/dev/apikey 获取）没有权限。
如果你只有普通 Web API Key，会收到 403 Forbidden。

对于日常搜索 App ID 的需求，resolve_steam_app_id 工具已改用免费的
Steam 商店公开搜索 API (store.steampowered.com/api/storesearch/)，
无需任何 API Key。此脚本仅用于构建完整的离线缓存。

Usage:
  python scripts/fetch_steam_app_list.py --api-key YOUR_STEAM_PUBLISHER_KEY
"""

import argparse
import json
import os
import sys
import urllib.parse
from pathlib import Path

import httpx


def get_api_key(cli_arg: str | None = None) -> str:
    """获取 Steam API Key: CLI 参数 > 环境变量 > settings.yaml"""
    if cli_arg:
        return cli_arg

    env_key = os.environ.get("STEAM_API_KEY", "")
    if env_key:
        return env_key

    # 尝试从 settings.yaml 读取
    try:
        import yaml
        settings_path = Path("config/settings.yaml")
        if settings_path.exists():
            raw = yaml.safe_load(settings_path.read_text(encoding="utf-8"))
            key = raw.get("steam", {}).get("api_key", "")
            # 解析 ${ENV_VAR} 占位符
            if key and not key.startswith("${"):
                return key
    except Exception:
        pass

    return ""


def main():
    parser = argparse.ArgumentParser(description="生成 Steam 游戏列表本地缓存")
    parser.add_argument("--api-key", help="Steam Web API Key")
    args = parser.parse_args()

    api_key = get_api_key(args.api_key)
    if not api_key:
        print("错误: 未找到 Steam API Key。请通过以下方式之一提供:")
        print("  1. 命令行: --api-key YOUR_KEY")
        print("  2. 环境变量: set STEAM_API_KEY=YOUR_KEY")
        print("  3. config/settings.yaml 中直接填写 api_key 值")
        return 1

    all_apps: list[dict] = []
    last_appid: int = 0
    page = 0

    client = httpx.Client(
        timeout=30,
        follow_redirects=True,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
        },
    )

    try:
        while True:
            page += 1
            params: dict = {}
            if page > 1:
                params["last_appid"] = last_appid
            params["max_results"] = 50000

            input_json = json.dumps(params)
            query = urllib.parse.urlencode(
                {"key": api_key, "input_json": input_json}, safe=""
            )

            url = f"https://partner.steam-api.com/IStoreService/GetAppList/v1/?{query}"
            print(f"第 {page} 页 (last_appid={last_appid})...")

            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()

            apps = data.get("response", {}).get("apps", [])
            if not apps:
                print("  没有更多数据")
                break

            all_apps.extend(apps)
            last_appid = apps[-1]["appid"]
            print(f"  获取 {len(apps):,} 条，累计 {len(all_apps):,} 条，最后 appid={last_appid}")

            if len(apps) < 50000:
                break

    except Exception as e:
        print(f"获取失败: {e}")
        if page == 1:
            return 1

    if not all_apps:
        print("未获取到任何数据")
        return 1

    cache_dir = Path("data")
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / "steam_app_list.json"
    # 转换为 [{app_id: int, name: str}] 格式（与 agent 工具兼容）
    normalized = [{"app_id": a["appid"], "name": a.get("name", "")} for a in all_apps]
    cache_file.write_text(json.dumps(normalized, ensure_ascii=False), encoding="utf-8")
    print(f"\n已缓存 {len(normalized):,} 个 Steam 游戏到 {cache_file}")

    # 验证：搜索测试
    test_name = "Counter-Strike 2"
    found = [a for a in normalized if test_name.lower() in a["name"].lower()][:3]
    if found:
        print(f"验证搜索 '{test_name}': {found}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
