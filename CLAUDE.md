# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 常用命令

```bash
# 安装项目（开发模式，含测试依赖）
pip install -e .[dev]
playwright install chromium

# 启动服务
autoflux                        # 或: python -m src.web.app

# 启动 Worker Agent（分布式执行节点，可选）
python scripts/worker_agent.py --base-url http://127.0.0.1:8000

# 前端开发（src/web/ 目录下）
cd src/web
npm install
npm run dev                     # Vite 开发服务器，端口 5173，API 代理到 :8000
npm run build                   # 生产构建，输出到 ../static/dist/

# 测试
pytest                          # 全部测试
pytest -m "not integration"     # 跳过集成测试
pytest tests/test_*.py -k worker  # 运行特定测试（如 worker 相关）
pytest tests/test_smoke.py      # 单个文件

# 代码检查
ruff check src/
ruff format src/
```

## 架构

**插件化 Pipeline 架构**，核心链路：

```
WebUI/API → Scheduler → WorkerClaimCoordinator → Pipeline → Collector(s) → Processor(s) → Storage(s)
                ↓                ↓                                ↓
          SessionRegistry  [Worker claim/release]   [SQLAlchemy: PostgreSQL/pgvector]
                                ↑
                          WorkerAgent(s) [可选分布式节点]
```

- **`src/core/registry.py`** — 全局单例 `ComponentRegistry`，通过 `@registry.register("collector", "steam")` 装饰器注册插件。启动时 `_auto_discover_plugins()` 扫描 `src/collectors/`、`src/processors/`、`src/storage/` 子包自动导入。
- **`src/core/pipeline.py`** — Pipeline 编排引擎，按序执行 collector → processor → storage。Storage 通过 `src/storage/factory.py` 的 `get_storage()` 工厂函数统一获取，根据 `config/settings.yaml` 中 `database.provider` 选择后端。
- **`src/core/scheduler.py`** — 异步调度器，信号量控制并发，APScheduler 驱动 cron 定时任务，任务/pipeline 持久化到 SQLAlchemy 存储用于重启恢复。任务失败时通过 `AlertService` 推送钉钉/Discord 通知。
- **`src/core/worker_claim_coordinator.py`** — 协调 Worker claim 任务生命周期，管理采集器会话状态、Worker 能力匹配、任务级 Worker 更新。支持本地执行和远程 Worker 分发。
- **`src/services/session_registry.py`** — 持久化采集器会话清单（账号绑定、登录态、Worker 能力需求），驱动任务路由决策。
- **`src/worker/agent.py`** — 可选的远程执行节点，轮询服务端 claim 任务、本地执行 Pipeline、上报结果。通过 `scripts/worker_agent.py` 启动，支持声明能力标签（如 `steamdb_profile`、`qimai_session`）。
- **`src/core/config.py`** — 从 `config/settings.yaml` 加载配置，支持 `${ENV_VAR}` 环境变量插值。数据库连接通过 `DATABASE_URL` 环境变量配置。

**Web 层**：FastAPI (`src/web/app.py`) + 纯 JS SPA 前端 (Vite + Tailwind 4 + ECharts)。前端源码在 `src/web/src/`，构建输出到 `src/web/static/dist/`。Vite 开发模式下 API 请求代理到后端 8000 端口。HTML 模板已拆分为 Jinja2 `{% include %}` 组件（`src/web/templates/components/` 和 `pages/`）。

**Agent 系统**：LangChain 驱动的对话助手 (`src/agent/`)，核心特性：
- **流式输出与中断**：SSE 推送，支持随时停止生成。
- **运行时配置**：WebUI 可视化切换 LLM provider（Qwen/DeepSeek/商汤/Ollama）和 Agent 类型（OpenAI tools / ReAct）。
- **MCP 浏览器集成**：通过 `src/agent/mcp_client.py` 集成 Playwright MCP Server（需 Node.js + `npx`），Agent 可自主导航网页、分析 DOM、执行 JS 提取，并创建动态采集 Pipeline（`src/collectors/dynamic_playwright_collector.py`）。
- **会话管理**：多会话隔离，消息持久化到 localStorage，支持创建/切换/重命名/删除会话。
- **工具集**：模块化工具在 `src/agent/tools/` 下，包括任务管理、数据浏览、语义搜索（`semantic_search.py`，pgvector 向量检索）、Pipeline 管理、报告生成、Steam App ID 解析等。

**报告**：`src/reporting/` 整合多源数据，LLM 生成分析文本，openpyxl 输出 Excel。支持提取器插件化（`src/reporting/extractors/`）。

**存储层**：基于 SQLAlchemy 的统一 ORM 层（`src/storage/models.py`）。`get_storage()` 工厂函数（`src/storage/factory.py`）根据配置返回对应的存储引擎（`sqlalchemy` / `sqlalchemy_scheduler`）。支持 PostgreSQL（含 pgvector 向量检索）。向量嵌入由 `VectorizerProcessor`（`src/processors/vectorizer.py`）通过 DashScope Embedding API 生成。

**告警**：`AlertService` 单例（`src/services/alert_service.py`），支持钉钉 Markdown、Discord Embed、通用 Webhook 三种消息类型，在任务执行失败时自动推送。

## 关键约定

- **所有 API 路由**挂载在 `/api` 前缀下，页面路由 (`src/web/routes/pages.py`) 无前缀。
- **前端是纯 JS SPA**，无框架。页面组件在 `src/web/src/pages/<name>/index.js`，核心模块在 `src/web/src/core/`。
- **服务层单例**（`TaskService`、`AgentService`、`ReportGenerator`）通过模块级懒加载函数获取，不走依赖注入容器。
- **调度器 `Scheduler`** 是全局单例，`src/web/app.py` 在 lifespan 中启动/停止它。
- **无 ORM 迁移**。SQLAlchemy 表由 `Base.metadata.create_all()` 在存储初始化时按需创建。`models.py` 定义了 `RecordModel` 和 `SchedulerStateModel`。
- **存储通过工厂获取**：所有代码通过 `get_storage()` 获取存储实例，不直接实例化具体类。存储后端由 `config/settings.yaml` 中的 `database.provider` 控制。
- **嵌入模型**：`get_embeddings()` 工厂函数（`src/services/_utils.py`）返回 DashScope Embeddings 实例，`vectorizer.py` 和 `semantic_search.py` 共用。
- **采集器需登录态**：SteamDB 通过 CDP 连接已登录 Chrome（`scripts/steamdb_login.py`），七麦通过 Playwright 持久化 profile（`scripts/qimai_login.py`）。Worker 通过能力标签（如 `steamdb_profile`）声明登录态资源，Scheduler 根据 `SessionRegistry` 路由任务到对应 Worker。

## 注意事项

- **Python 3.12+** 必需。Windows 下会自动切换到 `WindowsProactorEventLoopPolicy`（兼容 Playwright 子进程）。
- **Worker Agent 可选**：默认 Scheduler 本地执行所有任务。启动 Worker Agent 后，需要登录态的任务会自动路由到声明对应能力的 Worker。Worker 通过环境变量或命令行参数配置 `--base-url`、`--capability` 等。
- **Agent MCP 依赖**：Playwright MCP 工具需要 Node.js 和 `npx`。Windows 下 Playwright 不可用或连续失败时会自动降级并禁用浏览器工具。
- **前端有两套 JS 加载路径**：`index.html` 通过 Jinja2 条件分三路 — ① Vite dev (`VITE_DEV=1`, localhost:5173) ② Vite 构建产物 (`static/dist/`, 需 `npm run build`) ③ 静态脚本 (`static/agent.js` 等)。修改 `src/web/src/` 源码后必须 `npm run build`，否则 Vite 构建模式下不生效。
- **前端构建产物已提交到 git**（`src/web/static/dist/`），修改前端后记得 `npm run build` 再提交。
- **config/settings.yaml** 中的敏感值用 `${VAR_NAME}` 引用环境变量，不要直接写明文密钥。数据库连接通过 `DATABASE_URL` 环境变量配置。
- 集成测试标记为 `@pytest.mark.integration`，会访问外部服务，CI 环境需要跳过或配置凭据。
- 部分采集器（Google Trends、Twitch、Firecrawl）需要海外网络连通性。
- `pgvector` 为强制依赖（`models.py` 导入），但未安装时自动降级为 JSON 类型。
- **Edit 工具修改 HTML 模板**时，可能将属性引号保存为 `\"` 字面量，导致浏览器无法识别元素 ID。修改 `index.html` 后务必用 `grep` 或 `curl` 检查渲染输出。
