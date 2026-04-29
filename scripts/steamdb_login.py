"""Launch a local browser with CDP enabled for manual SteamDB login."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen


DEFAULT_PORT = 9222


def main() -> int:
    parser = argparse.ArgumentParser(description="Start Chrome/Edge for SteamDB login via CDP.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--browser", default="", help="Chrome/Edge executable path. Auto-detected when empty.")
    parser.add_argument("--profile-dir", default=str(Path.cwd() / "data" / "steamdb_profile"))
    args = parser.parse_args()

    browser_path = Path(args.browser) if args.browser else find_browser_executable()
    if not browser_path or not browser_path.exists():
        print("Could not find Chrome/Edge. Pass --browser C:\\path\\to\\chrome.exe")
        return 1

    profile_dir = Path(args.profile_dir)
    profile_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        str(browser_path),
        f"--remote-debugging-port={args.port}",
        f"--user-data-dir={profile_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        "https://steamdb.info/login/",
    ]
    print(f"Starting browser: {browser_path}")
    print(f"Profile dir: {profile_dir}")
    print(f"CDP endpoint: http://127.0.0.1:{args.port}")
    process = subprocess.Popen(cmd)

    print("Log in to SteamDB in the opened browser window.")
    input("Press Enter after login is complete. Keep the browser window open for collection...")

    if verify_cdp(args.port):
        print("CDP browser is reachable. You can now run SteamDB collection.")
        print("Do not close this browser while collection is running.")
        return 0

    print("Could not reach the CDP endpoint. Check the browser and port.")
    if process.poll() is not None:
        print(f"Browser process exited with code {process.returncode}.")
    return 2


def find_browser_executable() -> Path | None:
    candidates: list[Path] = []
    for env_name in ("CHROME_PATH", "EDGE_PATH"):
        env_value = os.environ.get(env_name, "").strip()
        if env_value:
            candidates.append(Path(env_value))
    for executable in ("chrome.exe", "msedge.exe"):
        resolved = shutil.which(executable)
        if resolved:
            candidates.append(Path(resolved))
    candidates.extend([
        Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
        Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
        Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
    ])
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def verify_cdp(port: int) -> bool:
    deadline = time.time() + 10
    url = f"http://127.0.0.1:{port}/json/version"
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=2) as response:
                return response.status == 200
        except Exception:
            time.sleep(0.5)
    return False


if __name__ == "__main__":
    sys.exit(main())
