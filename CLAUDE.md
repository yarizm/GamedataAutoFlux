# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 常用命令

```bash
# 安装项目（开发模式，含测试依赖）
pip install -e .[dev]
playwright install chromium

# 启动服务
autoflux                        # 或: python -m src.web.app

# 前端开发（src/web/ 目录下）
cd src/web
npm install
npm run dev                     # Vite 开发服务器，端口 5173，API 代理到 :8000
npm run build                   # 生产构建，输出到 ../static/dist/

# 测试
pytest                          # 全部测试
pytest -m "not integration"     # 跳过集成测试
pytest tests/test_smoke.py      # 单个文件

# 代码检查
ruff check src/
ruff format src/
```

## 架构

**插件化 Pipeline 架构**，核心链路：

```
WebUI/API → Scheduler → Pipeline → Collector(s) → Processor(s) → Storage(s)
                                                    ↓
                                     [SQLAlchemy: PostgreSQL/pgvector]
```

- **`src/core/registry.py`** — 全局单例 `ComponentRegistry`，通过 `@registry.register("collector", "steam")` 装饰器注册插件。启动时 `_auto_discover_plugins()` 扫描 `src/collectors/`、`src/processors/`、`src/storage/` 子包自动导入。
- **`src/core/pipeline.py`** — Pipeline 编排引擎，按序执行 collector → processor → storage。Storage 通过 `src/storage/factory.py` 的 `get_storage()` 工厂函数统一获取，根据 `config/settings.yaml` 中 `database.provider` 选择后端。
- **`src/core/scheduler.py`** — 异步调度器，信号量控制并发，APScheduler 驱动 cron 定时任务，任务/pipeline 持久化到 SQLAlchemy 存储用于重启恢复。任务失败时通过 `AlertService` 推送钉钉/Discord 通知。
- **`src/core/config.py`** — 从 `config/settings.yaml` 加载配置，支持 `${ENV_VAR}` 环境变量插值。数据库连接通过 `DATABASE_URL` 环境变量配置。

**Web 层**：FastAPI (`src/web/app.py`) + 纯 JS SPA 前端 (Vite + Tailwind 4 + ECharts)。前端源码在 `src/web/src/`，构建输出到 `src/web/static/dist/`。Vite 开发模式下 API 请求代理到后端 8000 端口。HTML 模板已拆分为 Jinja2 `{% include %}` 组件（`src/web/templates/components/` 和 `pages/`）。

**Agent**：LangChain 驱动的对话助手 (`src/agent/`)，SSE 流式输出，支持运行时切换 LLM provider（Qwen/DeepSeek/商汤/Ollama）和 Agent 类型（OpenAI tools / ReAct）。工具集模块化在 `src/agent/tools/` 下，含语义搜索（`semantic_search.py`，通过 pgvector 向量相似度检索）。通过 MCP 协议集成 Playwright 浏览器工具（`src/agent/mcp_client.py`），Agent 可自主探索网页并创建动态采集 Pipeline（`src/collectors/dynamic_playwright_collector.py`）。

**报告**：`src/reporting/` 整合多源数据，LLM 生成分析文本，openpyxl 输出 Excel。

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
- **采集器需登录态**：SteamDB 通过 CDP 连接已登录 Chrome（`scripts/steamdb_login.py`），七麦通过 Playwright 持久化 profile（`scripts/qimai_login.py`）。

## 注意事项

- **Python 3.12+** 必需。Windows 下会自动切换到 `WindowsProactorEventLoopPolicy`（兼容 Playwright 子进程）。
- **前端有两套 JS 加载路径**：`index.html` 通过 Jinja2 条件分三路 — ① Vite dev (`VITE_DEV=1`, localhost:5173) ② Vite 构建产物 (`static/dist/`, 需 `npm run build`) ③ 静态脚本 (`static/agent.js` 等)。修改 `src/web/src/` 源码后必须 `npm run build`，否则 Vite 构建模式下不生效。
- **前端构建产物已提交到 git**（`src/web/static/dist/`），修改前端后记得 `npm run build` 再提交。
- **config/settings.yaml** 中的敏感值用 `${VAR_NAME}` 引用环境变量，不要直接写明文密钥。数据库连接通过 `DATABASE_URL` 环境变量配置。
- 集成测试标记为 `@pytest.mark.integration`，会访问外部服务，CI 环境需要跳过或配置凭据。
- 部分采集器（Google Trends、Twitch、Firecrawl）需要海外网络连通性。
- `pgvector` 为强制依赖（`models.py` 导入），但未安装时自动降级为 JSON 类型。
- **Edit 工具修改 HTML 模板**时，可能将属性引号保存为 `\"` 字面量，导致浏览器无法识别元素 ID。修改 `index.html` 后务必用 `grep` 或 `curl` 检查渲染输出。
