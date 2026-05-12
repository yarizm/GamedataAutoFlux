"""实际试跑 GameIdentifierResolver —— 测试真实 API 调用"""
import asyncio
import sys
from pathlib import Path

# 确保项目根在 path
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))


async def main():
    from src.services.game_resolver import GameIdentifierResolver

    resolver = GameIdentifierResolver()
    await resolver.setup()

    # ── Steam ──────────────────────────────────────────
    print("=== Steam: Counter-Strike 2 ===")
    result = await resolver.resolve_steam("Counter-Strike 2")
    assert result is not None, "CS2 Steam 不应为 None"
    assert result.identifier == "730", f"期望 730, 实际 {result.identifier}"
    assert result.confidence.value == "high", f"期望 high, 实际 {result.confidence.value}"
    print(f"  app_id={result.identifier}, name={result.game_name}, confidence={result.confidence.value}")
    print(f"  candidates: {len(result.candidates)}个")
    print("  PASSED")

    print("\n=== Steam: Dota 2 ===")
    result = await resolver.resolve_steam("Dota 2")
    assert result is not None
    assert result.identifier == "570", f"期望 570, 实际 {result.identifier}"
    assert result.confidence.value == "high"
    print(f"  app_id={result.identifier}, name={result.game_name}, confidence={result.confidence.value}")
    print("  PASSED")

    print("\n=== Steam: 明日方舟 (非Steam游戏) ===")
    result = await resolver.resolve_steam("明日方舟")
    # 明日方舟没有 Steam 版，应返回 not_found 或 low confidence
    if result:
        print(f"  app_id={result.identifier}, confidence={result.confidence.value}, detail={result.detail}")
        assert result.confidence.value in ("low", "medium"), f"非Steam游戏不应为 high, 实际 {result.confidence.value}"
    else:
        print("  None (未找到)")
    print("  PASSED")

    # ── Monitor ────────────────────────────────────────
    print("\n=== Monitor: Counter-Strike 2 ===")
    result = await resolver.resolve_monitor_name("Counter-Strike 2")
    assert result is not None
    print(f"  siteurl={result.identifier}, confidence={result.confidence.value}")
    print("  PASSED")

    # ── resolve_all ────────────────────────────────────
    print("\n=== resolve_all: Counter-Strike 2 ===")
    result = await resolver.resolve_all("Counter-Strike 2")
    high = result.high_confidence()
    found = result.found_platforms()
    print(f"  high confidence: {high}")
    print(f"  found platforms: {found}")
    assert "steam" in high, "CS2 Steam 应在 HIGH 中"
    print(f"  steam.app_id={result.steam.identifier}, steam.confidence={result.steam.confidence.value}")
    if result.monitor:
        print(f"  monitor.siteurl={result.monitor.identifier}, monitor.confidence={result.monitor.confidence.value}")
    if result.taptap:
        print(f"  taptap.id={result.taptap.identifier}, taptap.confidence={result.taptap.confidence.value}")
    if result.gtrends:
        print(f"  gtrends.keyword={result.gtrends.identifier}")
    print("  PASSED")

    print("\n=== resolve_all: Dota 2 ===")
    result = await resolver.resolve_all("Dota 2")
    assert result.steam and result.steam.identifier == "570"
    print(f"  steam={result.steam.identifier}, confidence={result.steam.confidence.value}")
    print("  PASSED")

    await resolver.teardown()

    print("\n" + "=" * 50)
    print("  全部 8 项断言通过")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
