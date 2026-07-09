"""Pipeline template definitions — shared data usable by routes, services, and agent tools."""

PIPELINE_TEMPLATES = [
    {
        "id": "steam_basic",
        "name": "Steam 基础采集",
        "description": "Steam -> cleaner -> sqlalchemy，适合保存清洗后的采集结果",
        "steps": [
            {"type": "collector", "name": "steam", "config": {"request_delay": 0.5}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "sqlalchemy", "config": {}},
        ],
    },
    {
        "id": "taptap_basic",
        "name": "TapTap 基础采集",
        "description": "TapTap -> cleaner -> sqlalchemy，适合公开页详情、评价、更新采集",
        "steps": [
            {"type": "collector", "name": "taptap", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "sqlalchemy", "config": {}},
        ],
    },
    {
        "id": "steam_discussions_basic",
        "name": "Steam Community 讨论采集",
        "description": "steam_discussions -> cleaner -> sqlalchemy，适合按时间区间采集玩家讨论",
        "steps": [
            {"type": "collector", "name": "steam_discussions", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "sqlalchemy", "config": {}},
        ],
    },
    {
        "id": "gtrends_basic",
        "name": "Google Trends 基础采集",
        "description": "gtrends -> cleaner -> sqlalchemy，适合获取游戏时序热度",
        "steps": [
            {"type": "collector", "name": "gtrends", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "sqlalchemy", "config": {}},
        ],
    },
    {
        "id": "monitor_basic",
        "name": "Monitor 基础采集",
        "description": "monitor -> cleaner -> sqlalchemy，适合 Steam 外围趋势指标采集",
        "steps": [
            {"type": "collector", "name": "monitor", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "sqlalchemy", "config": {}},
        ],
    },
    {
        "id": "qimai_basic",
        "name": "Qimai(七麦) 基础采集",
        "description": "qimai -> cleaner -> sqlalchemy，适合 AppStore 排名评分获取",
        "steps": [
            {"type": "collector", "name": "qimai", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "sqlalchemy", "config": {}},
        ],
    },
    {
        "id": "official_site_basic",
        "name": "游戏官网基础采集",
        "description": "official_site -> cleaner -> sqlalchemy，适合官网新闻、公告、版本更新和活动采集",
        "steps": [
            {"type": "collector", "name": "official_site", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "sqlalchemy", "config": {}},
        ],
    },
    {
        "id": "dynamic_playwright_basic",
        "name": "动态浏览器网页采集",
        "description": "dynamic_playwright -> cleaner -> sqlalchemy，Agent 探索网页后自动生成的配置采集流",
        "steps": [
            {"type": "collector", "name": "dynamic_playwright", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "sqlalchemy", "config": {}},
        ],
    },
]
