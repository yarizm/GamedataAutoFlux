# GamedataAutoFlux

基于 Python 的游戏数据采集与分析系统，提供 FastAPI WebUI，用于配置 Pipeline、执行任务、浏览数据和生成 Excel 报告。支持分布式 Worker 节点扩展。

## 核心功能

- **多数据源采集**：Steam（官方 API + SteamDB + Discussions）、TapTap、七麦数据、Google Trends、YouTube Data API、官网新闻、动态 Playwright 网页采集
- **Pipeline 编排**：DAG 执行引擎支持多源并行汇合、条件分支/故障转移、节点间数据依赖传递、可复用子图；兼容 Builder 模式三段式 Pipeline，支持断点恢复
- **AI Agent 对话助手**：LangChain + LangGraph 驱动，支持 Playwright MCP 浏览器工具，自然语言操控全系统
- **Smart Collector**：LLM 辅助 HTML 提取/验证，低于置信度阈值自动降级
- **报告生成**：LLM 分析文本 + openpyxl 输出 Excel，提取器插件化
- **调度与告警**：APScheduler cron 定时任务，钉钉 / Discord / Webhook 失败通知
- **任务可观测性**：结构化事件日志、Artifacts、Checkpoint 快照、WebSocket 实时推送
- **分布式 Worker**：可选 `worker_claim` 执行后端，Worker Agent 按 capability / session 匹配领取任务

## 安装与启动

### 方式一：Docker 部署（推荐）

```bash
cp .env.example .env
# 编辑 .env 填写必要的 API Key
docker-compose up -d
```

访问 `http://localhost:8000`。包含 PostgreSQL 15 + pgvector，自动建表。

### 方式二：本地运行

环境要求：Python >= 3.12、Chromium/Chrome/Edge（Playwright 需要）

```powershell
cd GamedataAutoFlux
python -m venv .venv
.\.venv\Scripts\activate        # Windows
# source .venv/bin/activate     # Linux/macOS

pip install -e .[dev]
playwright install chromium

# 启动服务
autoflux
# 或 python -m src.web.app
```

### 启动 Worker Agent（可选）

Worker Agent 可在任意网络可达的机器上运行，执行分发过来的采集任务：

```powershell
python scripts/worker_agent.py --base-url http://127.0.0.1:8000

# 声明 capability 以接收需要登录态的任务
python scripts/worker_agent.py --base-url http://192.168.1.100:8000 --capability steamdb_profile --capability qimai_session
```

Worker Agent 启动后自动注册、发送心跳（15s）、轮询领取任务（3s），支持 draining 优雅退出。

## 环境变量

主配置文件 `config/settings.yaml`，敏感值用 `${VAR_NAME}` 引用环境变量。

常用环境变量（`.env.example`）：

| 变量 | 用途 |
|------|------|
| `DATABASE_URL` | 数据库连接串，默认 `postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux` |
| `STEAM_API_KEY` | Steam Web API 密钥 |
| `DASHSCOPE_API_KEY` | 阿里云 DashScope（Qwen 模型 + Embedding） |
| `DEEPSEEK_API_KEY` | DeepSeek API |
| `SENSE_API_KEY` | 商汤 SenseNova |
| `FIRECRAWL_API_KEY` | 网页抓取 fallback 服务 |
| `YOUTUBE_API_KEY_1` / `YOUTUBE_API_KEY_2` | YouTube Data API 密钥（多 key 轮转） |

## 架构

```
WebUI/API → Scheduler → [in_process / worker_claim] → DAGExecutor → Collector(s) → Processor(s) → Storage
                ↕                               ↕
        EventBus → Hooks              WorkerClaimCoordinator
        (WebSocket/Alert/Report)          ↕
                                    WorkerAgent(s)
```

Pipeline 与 DAG 并存：`Pipeline.execute()` 默认委托 `DAGExecutor`（经 `pipeline_to_dag` 转换），支持多源并行汇合、条件分支/故障转移、节点间数据依赖传递、可复用子图四类编排场景。

### 核心模块

| 模块 | 路径 | 职责 |
|------|------|------|
| 注册中心 | `src/core/registry.py` | `@registry.register()` 装饰器注册 collector/processor/storage 插件 |
| 配置管理 | `src/core/config.py` | YAML 加载 + `${ENV}` 插值 + 按 section 写回 |
| Pipeline 引擎 | `src/core/pipeline.py` | Builder 模式编排，`execute()` 委托 DAGExecutor（可开关回退三段式） |
| DAG 引擎 | `src/core/dag.py` / `dag_executor.py` / `dag_nodes.py` / `dag_conditions.py` | 通用 DAG：拓扑分层并发、端口命名数据传递、条件边、子图展开、checkpoint 恢复 |
| 调度器 | `src/core/scheduler.py` | 信号量并发控制、in_process/worker_claim 双后端、cron 定时 |
| 事件总线 | `src/core/events.py` | 按 priority 分组的异步 pub/sub，解耦副作用 |
| 生命周期钩子 | `src/core/hooks.py` | ReportGeneration / Alert / WebSocket 广播 |
| 错误分类 | `src/core/errors.py` | 9 种错误码，自动推断 + 中文说明 + 修复建议 |
| 脱敏 | `src/core/sensitive.py` | 全局递归脱敏，覆盖 API Key / Cookie / Token |

### 采集器

所有采集器继承 `BaseCollector`（`src/collectors/base.py`），生命周期 `setup() → collect_batch() → teardown()`，内置并发控制、超时、自动重试。

| 采集器 | 数据源 | 特点 |
|--------|--------|------|
| Steam | 官方 API + SteamDB CDP | 商店数据、评测趋势、SteamDB charts/关注数/历史价格 |
| Steam Discussions | Steam 社区论坛 | 帖子与回复抓取 |
| TapTap | tapTap.cn | Playwright 浏览器抓取，可选 Firecrawl fallback |
| 七麦 | qimai.cn | Playwright 持久化 profile 登录态 |
| Google Trends | trends.google.com | pytrends API，支持代理 |
| YouTube Profiles | YouTube Data API | 频道元数据（订阅数、视频数） |
| YouTube Comments | YouTube Data API | 视频评论（top + scan 模式），多 key 轮转 |
| Official Site | 各游戏官网 | HTTP + Playwright 混合，16 款游戏 recipe 配置 |
| Monitor | Steam CCU + Twitch | 综合指标监控 |
| Dynamic Playwright | 任意网页 | Agent 动态生成 JS 提取脚本执行 |

### Agent 系统

LangChain + LangGraph 双运行时，支持运行时切换（`agent.runtime_backend`）。

- **工具集**：任务管理、数据浏览/搜索、语义搜索（pgvector）、Pipeline 管理、Cron 管理、报告生成/预检、Steam App ID 解析、游戏标识符发现、系统/就绪诊断
- **6 条规则图式工作流**（命中后走固定路径 + path bar + `result_card`，非 LLM 路由）：
  1. 报告（task 复查 → 预检 → 生成）
  2. 任务诊断（复查；显式重试语可 auto_retry）
  3. 动态 Pipeline（URL + 采集意图 → 创建）
  4. 系统就绪（配置 + 会话；默认不 deep probe）
  5. 定时任务（list / 草案创建 / 删除；**同句确认才写入**）
  6. 多源采集（游戏 + 多数据源草案；**同句确认才提交任务**）
- **SSE**：`thinking` / `tool_*` / `final` / `error`，路径工作流另有 `workflow_start|step|end` 与 `result_card`（navigate + copy）
- **Playwright MCP**：Agent 可自主导航网页、分析 DOM、执行 JS 提取（需 Node.js + npx）
- **会话持久化**：SQLAlchemy 存储，支持 thread 语义、LangGraph checkpoint（memory/file）

示例话术：

```text
有哪些定时任务
每天 8 点跑 pipeline:steam_full
确认创建 每天 8 点跑 pipeline:steam_full 名称 steam_daily
多源采集《原神》 steam 七麦
确认创建 多源采集《原神》 steam 七麦
检查七麦采集是否就绪
对任务 task:xxx 做报告预检
```

### 存储层

SQLAlchemy ORM，3 张表：

- `records` — 采集数据（key, source, collector, game_name, data, embedding, tags）
- `scheduler_states` — 调度器状态（任务/Pipeline/Cron/Worker 快照）
- `agent_sessions` — Agent 对话消息

`get_storage()` 全局单例工厂。pgvector 可选，缺失时自动降级为 JSON。

### 报告系统

LLM 分析文本 + openpyxl 输出 Excel。提取器插件化（`src/reporting/extractors/`），各数据源独立提取器。支持数据质量检查和预检（报告前验数据完整度）。

### Web 层

- **后端**：FastAPI，所有 API 挂载 `/api`，管理路由需 X-API-Key（本地免认证）
- **前端**：Vite + Tailwind 4 + ECharts，纯 JS SPA（无框架），8 个页面模块
- **安全**：SSRF 防护（禁止 localhost/内网 IP）、DNS rebinding 二次校验、敏感字段脱敏
- **WebSocket**：任务状态和结构化事件实时推送

## 高级采集：维护登录态

部分数据源需要浏览器登录态：

### SteamDB

通过 CDP 连接已登录 Chrome：

```powershell
# 方式一：命令行
python scripts/steamdb_login.py --port 9222

# 方式二：WebUI 系统面板一键启动
```

在弹出的浏览器中手动登录 SteamDB，保持窗口开启。

### 七麦数据

首次使用需要手动登录生成持久化 profile：

```powershell
python scripts/qimai_login.py
```

弹出的浏览器中完成登录后关闭，后续任务自动复用。

## 目录约定

| 目录 | 用途 |
|------|------|
| `config/` | settings.yaml + logging.yaml |
| `src/` | 全部源码 |
| `scripts/` | 工具脚本（登录、smoke test、批量报告） |
| `tests/` | pytest 测试（含 Agent workflow 与集成测试） |
| `data/` | 浏览器 profile、报告输出、缓存 |
| `logs/` | 运行日志 |
| `tmp/` | 临时文件（Excel 报告等） |
| `docs/` | 设计文档（过程产物，默认不随功能提交） |

## 测试

```bash
pytest -m "not integration"     # 默认/CI：跳过访问外网的集成测试
pytest                          # 全部测试（含 integration）
pytest tests/test_agent_workflow_cron.py tests/test_agent_workflow_multisource.py
pytest tests/test_worker_agent.py
```

测试隔离：`conftest.py` 的 `isolated_db_config` fixture 为每个测试创建临时 SQLite 数据库。

CI（`.github/workflows/ci.yml`）：`compileall` → `ruff` → Agent workflow 套件 → `pytest -m "not integration"` → 前端 `npm ci && npm run build`。

## 注意事项

- **Python 3.12+** 必需。Windows 自动切换 ProactorEventLoop。
- **config/settings.yaml** 中的敏感值必须用 `${VAR_NAME}` 引用环境变量，不要写明文。
- 部分采集器需要外网（Google Trends、Twitch、Firecrawl）。
- 前端修改 `src/web/src/` 后需 `npm run build`；`src/web/static/dist/` 默认 gitignore，CI/部署时构建。
- 集成测试标记 `@pytest.mark.integration`，CI 默认跳过。
- Agent 创建定时任务 / 多源提交等高风险操作需要**同一条消息**内的确认语（如「确认创建」）。

## 许可证

[MIT License](LICENSE)

## 免责声明

请在遵守目标站点服务条款、账号规则和当地法律法规的前提下使用本项目。
