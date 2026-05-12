"""Pipeline template definitions — shared data usable by routes, services, and agent tools."""

PIPELINE_TEMPLATES = [
    {
        "id": "steam_basic",
        "name": "Steam 基础采集",
        "description": "Steam -> cleaner -> local，适合保存清洗后的采集结果",
        "steps": [
            {"type": "collector", "name": "steam", "config": {"request_delay": 0.5}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "steam_vector_report",
        "name": "Steam 检索报告链路",
        "description": "Steam -> cleaner -> embedding -> vector，适合语义检索与报告",
        "steps": [
            {"type": "collector", "name": "steam", "config": {"request_delay": 0.5}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "taptap_basic",
        "name": "TapTap 基础采集",
        "description": "TapTap -> cleaner -> local，适合公开页详情、评价、更新采集",
        "steps": [
            {"type": "collector", "name": "taptap", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "taptap_report",
        "name": "TapTap 检索报告链路",
        "description": "TapTap -> cleaner -> embedding -> vector，适合移动端游戏检索与报告",
        "steps": [
            {"type": "collector", "name": "taptap", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "steam_full_report",
        "name": "Steam 一条龙报告",
        "description": "Steam -> cleaner -> embedding -> local -> vector，适合采集、落库和报告一条龙",
        "steps": [
            {"type": "collector", "name": "steam", "config": {"request_delay": 0.5}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "steam_discussions_basic",
        "name": "Steam Community 讨论采集",
        "description": "steam_discussions -> cleaner -> local，适合按时间区间采集玩家讨论",
        "steps": [
            {"type": "collector", "name": "steam_discussions", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "steam_discussions_report",
        "name": "Steam Community 讨论检索报告链路",
        "description": "steam_discussions -> cleaner -> embedding -> vector，适合讨论语义检索与报告",
        "steps": [
            {"type": "collector", "name": "steam_discussions", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "steam_discussions_full_report",
        "name": "Steam Community 讨论一条龙报告",
        "description": "steam_discussions -> cleaner -> embedding -> local -> vector，适合讨论采集、落库和自动报告",
        "steps": [
            {"type": "collector", "name": "steam_discussions", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "taptap_full_report",
        "name": "TapTap 一条龙报告",
        "description": "TapTap -> cleaner -> embedding -> local -> vector，适合公开页采集、落库和报告",
        "steps": [
            {"type": "collector", "name": "taptap", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "gtrends_basic",
        "name": "Google Trends 基础采集",
        "description": "gtrends -> cleaner -> local，适合获取游戏时序热度",
        "steps": [
            {"type": "collector", "name": "gtrends", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "monitor_basic",
        "name": "Monitor 基础采集",
        "description": "monitor -> cleaner -> local，适合 Steam 外围趋势指标采集",
        "steps": [
            {"type": "collector", "name": "monitor", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "monitor_report",
        "name": "Monitor 检索报告链路",
        "description": "monitor -> cleaner -> embedding -> vector，适合趋势检索与报告",
        "steps": [
            {"type": "collector", "name": "monitor", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "monitor_full_report",
        "name": "Monitor 一条龙报告",
        "description": "monitor -> cleaner -> embedding -> local -> vector，适合采集、落库和自动报告",
        "steps": [
            {"type": "collector", "name": "monitor", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "qimai_basic",
        "name": "Qimai(七麦) 基础采集",
        "description": "qimai -> cleaner -> local，适合 AppStore 排名评分获取",
        "steps": [
            {"type": "collector", "name": "qimai", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "qimai_report",
        "name": "Qimai(七麦) 检索报告链路",
        "description": "qimai -> cleaner -> embedding -> vector，适合 AppStore 排名报告",
        "steps": [
            {"type": "collector", "name": "qimai", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
    {
        "id": "official_site_basic",
        "name": "游戏官网基础采集",
        "description": "official_site -> cleaner -> local，适合官网新闻、公告、版本更新和活动采集",
        "steps": [
            {"type": "collector", "name": "official_site", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
        ],
    },
    {
        "id": "official_site_full_report",
        "name": "游戏官网检索报告链路",
        "description": "official_site -> cleaner -> embedding -> local -> vector，适合官网正文检索与报告",
        "steps": [
            {"type": "collector", "name": "official_site", "config": {}},
            {"type": "processor", "name": "cleaner", "config": {}},
            {"type": "processor", "name": "embedding", "config": {}},
            {"type": "storage", "name": "local", "config": {}},
            {"type": "storage", "name": "vector", "config": {}},
        ],
    },
]
