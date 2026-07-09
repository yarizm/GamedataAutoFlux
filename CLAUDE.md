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

## 架构概览

**三层核心链路**：

```
WebUI/API → Scheduler → [in_process / worker_claim] → DAGExecutor → Collector(s) → Processor(s) → Storage
                ↕                               ↕
        EventBus → Hooks              WorkerClaimCoordinator
        (WebSocket/Alert/Report)           ↕
                                    WorkerAgent(s) [可选分布式节点]
```

Pipeline 与 DAG 并存：`Pipeline.execute()` 默认内部委托 `DAGExecutor`（`pipeline_to_dag` 转换后跑通用 DAG 引擎），失败回退原三段式逻辑。DAG 支持多源并行汇合、条件分支/故障转移、节点间数据依赖、可复用子图四类场景。

### 1. 核心引擎 (`src/core/`)

- **`registry.py`** — 全局单例 `ComponentRegistry`。通过 `@registry.register("collector", "steam")` 装饰器注册插件。启动时 `app.py` 的 `_auto_discover_plugins()` 扫描 `src/collectors/`、`src/processors/`、`src/storage/` 子包自动导入。
- **`config.py`** — YAML 配置加载，支持 `${ENV_VAR}` 环境变量插值和 `SERVER_HOST`/`SERVER_PORT` 直接覆盖。`get(key, default)` 用点号路径取值。`save_section()` 可写回 settings.yaml 保留原有格式。自动加载 `.env` 文件。
- **`config_schema.py`** — settings.yaml 的 schema 验证，启动时自动运行。
- **`pipeline.py`** — Builder 模式的 Pipeline 编排引擎。`execute()` 默认委托 DAGExecutor（受 `pipeline.use_dag_execution` 配置开关控制，默认开启），DAG 失败回退 `_execute_legacy` 三段式。保留 `to_config()`/`from_config()`/`add_collector|processor|storage` builder API。
- **`pipeline_recovery.py`** — Checkpoint 恢复逻辑。`build_pipeline_recovery_context` 从 checkpoint 的 cursor/state 计算 `collect` 上下文，`apply_collect_resume_context` 跳过已完成 target。`build_storage_record_key` 生成键 `{task_id}:{source}:{resume_run_index}:{sequence}`。`build_pipeline_resume_state` 产出 pipeline 格式 resume_state（`target_order`/`next_target_index`/`completed_targets`/`output_record_keys`）。
- **`dag.py`** — DAG 数据结构：`PortSpec`/`NodeSpec`/`Edge`/`Condition`/`DAG`/`DAGResult`，`to_storage()`/`from_storage()` 序列化，`pipeline_to_dag()` 把三段式 Pipeline 转等价 DAG。
- **`dag_nodes.py`** — 节点包装层：`NodeContext`/`NodeProtocol`/`CollectorNode`/`ProcessorNode`/`StorageNode`。复用 pipeline 的 `build_storage_record_key`/`_build_storage_metadata` 保证存储键/metadata 与 Pipeline 路径一致。
- **`dag_executor.py`** — `DAGExecutor`：拓扑排序（Kahn 分层）+ asyncio.gather 并发 + 端口表（多入边同端口聚合 list）+ 条件边求值（边级抑制，不污染共享端口）+ checkpoint 恢复（消费 pipeline 格式 recovery_context）+ composite 节点子图展开 + 节点生命周期事件。collector 层全失败早终止。
- **`dag_conditions.py`** — 预置条件谓词：`on_success`/`on_failure`/`on_nonempty`/`on_empty` + `CONDITION_PREDICATES` 注册表。
- **`scheduler.py`** — 异步调度器，信号量控制并发，支持 `in_process` 和 `worker_claim` 两种执行后端。任务通过 Semaphore 限制并发（默认 5）。委托给 4 个协调器/服务：
  - `TaskExecutionCoordinator` — 进程内任务执行、重试、checkpoint
  - `WorkerClaimCoordinator` — Worker 领取/完成/失败任务的协调
  - `SchedulerStateService` — 任务/Pipeline/Cron 持久化与恢复
  - `SchedulerCronService` — APScheduler cron 定时任务管理
  - `TaskObservabilityService` — 事件/产物/checkpoint 的记录与查询
- **`events.py`** — `EventBus` 发布/订阅机制，按 priority 分组，同组并发执行，组间顺序。单处理器异常不阻塞其他。4 种事件类型：`task_updated`、`task_completed`、`task_event`。
- **`hooks.py`** — 事件驱动的副作用处理：
  - `ReportGenerationHook` — task_completed → 自动生成报告
  - `AlertHook` — task_completed 失败 → 钉钉/Discord/Webhook 告警
  - `WebSocketBroadcastHook` — task_updated → WebSocket 广播任务状态
  - `WebSocketTaskEventHook` — task_event → WebSocket 广播结构化事件
- **`task.py`** — 任务状态机：PENDING → RUNNING → SUCCESS / FAILED → RETRYING。支持 `first_started_at`（跨重试保留）和 `total_duration_seconds`。结果摘要与完整结果分离存储。
- **`errors.py`** — 统一错误码枚举 `ErrorCode`（9 种），带中文标签、建议和严重级别。`classify_exception()` 根据异常消息自动推断错误码。
- **`collector_metadata.py`** — 每个 collector 的声明式元数据：capability 列表、session 需求、checkpoint 支持级别（L0-L3）、target 验证规则、worker binding 模式。
- **`session_runtime.py`** — 浏览器会话的运行时建模：account 信息、session state（health check）、lease 策略、worker binding。
- **`sensitive.py`** — 全局脱敏工具，`redact_sensitive()` 递归脱敏字典中的敏感字段。

### 2. 采集器 (`src/collectors/`)

- **`base.py`** — `BaseCollector` 抽象基类。生命周期：`setup()` → `collect()`/`collect_batch()` → `teardown()`。`collect_batch()` 内置并发控制、超时、自动重试（可重试错误码：network_unreachable、rate_limited、unknown）。
- **Steam** (`steam_collector.py` + `steam/` 子包) — Steam 官方 API（商店数据、评测）+ SteamDB 浏览器抓取（CDP 连接已登录 Chrome）。支持 rate limiting、人类行为模拟。
- **Steam Discussions** (`steam_discussions_collector.py`) — Steam 社区论坛帖子抓取。
- **TapTap** (`taptap_collector.py` + `taptap/` 子包) — Playwright 浏览器抓取 + 可选 Firecrawl fallback。
- **七麦/Qimai** (`qimai_collector.py`) — Playwright 持久化 profile 登录态，CDP 可选。
- **Google Trends** (`gtrends_collector.py` + `gtrends/` 子包) — pytrends API。
- **YouTube** (`youtube/` 子包) — 最新添加。两个采集器：
  - `youtube_profiles` — 频道元数据（订阅数、视频数等）
  - `youtube_comments` — 视频评论抓取（top + scan 模式）
  - 含 client_pool（多 API key 轮转）、rate limiting
- **官网/Official Site** (`official_site_collector.py`) — HTTP + Playwright 混合，支持 recipes 配置（游戏 → 官网 URL 映射），按 include/exclude pattern 过滤。
- **Monitor** (`monitor_collector.py`) — Steam CCU + Twitch 指标综合监控。
- **Dynamic Playwright** (`dynamic_playwright_collector.py`) — Agent 动态生成的 JS 提取脚本执行。
- **LLM Extractor** (`llm_extractor.py`) — LLM 辅助结构化数据提取，置信度阈值自动降级。
- **HTML Trimmer** (`html_trimmer.py`) — HTML 压缩/截断工具。

### 3. Agent 系统 (`src/agent/`)

LangChain + LangGraph 驱动，是最复杂的子系统：

- **`agent.py`** — `AgentService` 核心，管理 Agent 生命周期、会话、工具注册。
- **`runtime.py`** — `langgraph_agent` / `langchain_classic` 两种运行时后端切换。
- **`agent_invoke_lifecycle.py`** — 调用生命周期：setup → execute → teardown，含错误处理和中途取消。
- **`agent_invoke_orchestration.py`** — 编排层，协调 prompt 构建、工具调用、结果处理。
- **`agent_invoke_stream.py`** — SSE 流式输出，支持中断和恢复。
- **`agent_stream_events.py`** — 流式事件类型定义（tool_call / tool_result / final / error）。
- **`stream_parser.py`** — 流式响应解析。
- **`checkpointer.py`** — LangGraph checkpoint 持久化，支持 memory/file 后端。
- **`thread_store.py`** — 线程历史持久化与恢复。
- **`mcp_client.py`** — Playwright MCP Server 集成（需 Node.js + npx），Agent 可操作浏览器。
- **`agent_prompting.py`** — System prompt 构建，含工具描述和规则注入。
- **`agent_redaction.py`** — 工具调用参数的脱敏。
- **`agent_history_state.py`** — 会话历史的状态管理。
- **`agent_status_summary.py`** — Agent 状态摘要生成。
- **`schemas.py`** — Pydantic 模型（ChatRequest, AgentConfig 等）。
- **Workflow 图系统** — LangGraph 工作流引擎：
  - `workflow_graphs.py` — 图定义（tool bridge 构建器）
  - `workflow_types.py` — 状态/节点/边类型定义
  - `workflow_routing.py` — 用户意图 → workflow 路由匹配
  - `workflow_matchers.py` — 路由匹配规则
  - `workflow_runtime_nodes.py` — 运行时节点处理器
  - `workflow_bridge_events.py` — 图节点 → SSE 事件桥接
  - `workflow_responses.py` — Workflow 结果响应构建
  - `workflows.py` — Agent 工具注册到 workflow 的映射
  - `workflow_support.py` — 支持函数

**三条优先图式工作流**：
1. 报告链路：任务详情 → 采集结果复查 → 报告预检 → 报告生成
2. 任务诊断链路：任务详情 → 采集结果复查 → 自动重试决策
3. 动态 Pipeline 链路：URL 识别 → 采集草案生成 → create_dynamic_pipeline

**工具集** (`src/agent/tools/`)：
- `tasks.py` — 创建/查询/取消任务
- `data.py` — 数据浏览与搜索
- `semantic_search.py` — pgvector 语义检索
- `pipelines.py` — Pipeline 管理（含 create_dynamic_pipeline）
- `reports.py` — 报告生成与预检
- `cron.py` — 定时任务管理
- `identifiers.py` — Steam App ID 解析和游戏标识符发现
- `system.py` — 系统状态查询
- `utils.py` — 工具辅助函数

### 4. Web 层 (`src/web/`)

- **`app.py`** — FastAPI 应用入口。lifespan 中：自动发现插件 → 初始化 DB session factory → 初始化 storage → 创建 Agent 会话服务 → 注入 repositories 到 Scheduler → 注册 EventBus hooks → 注册 YouTube pipeline 模板 → 启动 Scheduler。所有 API 路由挂载在 `/api`，管理端路由需要 `require_admin` 依赖。
- **路由**：
  - `tasks.py` — 任务 CRUD、提交、取消、事件/产物/checkpoint 查询
  - `pipelines.py` — Pipeline CRUD、模板管理
  - `reports.py` — Excel 报告生成与下载
  - `data.py` — 数据记录浏览、搜索、删除
  - `agent.py` — Agent 聊天（SSE）、会话管理、状态查询
  - `workers.py` — Worker 注册、心跳、claim/complete/fail
  - `health.py` — 健康检查、诊断信息
  - `targets.py` — 采集目标建议发现
  - `youtube_export.py` — YouTube 数据 XLSX 导出
  - `ws.py` — WebSocket 连接管理（任务状态/事件实时推送）
  - `pages.py` — 页面路由（无 `/api` 前缀）
- **`safety.py`** — 安全层：
  - `require_admin` — 本地请求免认证，远程需要 X-API-Key
  - `validate_dynamic_playwright_config` — 禁止访问 localhost/内网 IP（SSRF 防护）
  - `validate_url_runtime` — 运行时 DNS rebinding 二次校验
- **前端** — Vite + Tailwind 4 + ECharts，纯 JS SPA。源码在 `src/web/src/`，构建输出到 `src/web/static/dist/`。

### 5. 服务层 (`src/services/`)

所有服务通过模块级懒加载函数获取，不走依赖注入容器。

- **`task_service.py`** — 任务生命周期管理（创建、提交、取消）。依赖 Scheduler 和 SessionRegistry。
- **`worker_registry.py`** — Worker 注册/心跳/状态管理。有 `InMemoryWorkerRegistry` 和 `StorageWorkerRegistry` 两种实现。
- **`session_registry.py`** — 持久化采集器会话清单（账号绑定、登录态、Worker 能力需求），驱动任务路由。同样有内存和持久化两种实现。
- **`agent_session_service.py`** — Agent 多会话管理，消息持久化到 SQLAlchemy。
- **`alert_service.py`** — 告警单例，支持钉钉 Markdown、Discord Embed、通用 Webhook。
- **`data_browser_service.py`** — 数据浏览与搜索逻辑。
- **`data_management_service.py`** — 数据删除与管理。
- **`game_resolver.py`** — 游戏名称 → 平台标识符解析。
- **`task_precheck_service.py`** — 任务提交前校验（target 参数完整性、session 可用性）。
- **Repository 层** — SQLAlchemy 实现：
  - `sqlalchemy_task_repository.py`
  - `sqlalchemy_cron_repository.py`
  - `sqlalchemy_pipeline_repository.py` — Pipeline 快照（`state_type="pipeline"`），`load_as_dag()` 优先读 graph 回退 pipeline 自动转换
  - `sqlalchemy_dag_repository.py` — DAG 图定义持久化（`state_type="graph"`，key `graph:{name}`）
- **可观测性服务**：
  - `task_event_service.py` — 结构化事件存储/查询
  - `task_artifact_service.py` — 产物存储（如 Excel 报告）
  - `task_checkpoint_service.py` — Checkpoint 存储/查询
- **`_utils.py`** — 共享工具函数（如 `get_embeddings()`）。

### 6. 存储层 (`src/storage/`)

- **`base.py`** — `BaseStorage` 抽象接口和 `StorageRecord` 数据模型。
- **`factory.py`** — `get_storage()` 全局单例工厂。当前架构共享同一个 SQLAlchemy 实例。
- **`models.py`** — 三个 ORM 模型：
  - `RecordModel` — 采集数据记录（key, source, collector, game_name, data, embedding, tags）
  - `SchedulerStateModel` — 调度器状态（任务/Pipeline/Cron/Worker 快照）
  - `AgentSessionModel` — Agent 会话消息
- **`session_factory.py`** — 共享的 SQLAlchemy `AsyncSession` 工厂，支持 pgvector 自动降级。
- **`sqlalchemy_store.py`** — 数据记录存储实现。
- **`sqlalchemy_scheduler_store.py`** — 调度器状态存储实现。

### 7. 报告 (`src/reporting/`)

- **`generator.py`** — `ReportGenerator`，LLM 生成分析文本 + openpyxl 输出 Excel。
- **`data_extractor.py`** — 从存储记录中按 template 提取报告所需字段。
- **`excel_exporter.py`** — Excel 文件生成与格式化。
- **`report_templates.py`** — 报告模板定义。
- **`quality.py`** — 数据质量检查。
- **`extractors/`** — 插件化提取器（steam、steam_discussions、qimai、basic_sources、common），每个负责从特定数据源格式提取结构化表格。

### 8. Worker Agent (`src/worker/agent.py`)

独立的远程执行节点，通过 REST API 与主服务通信：
- 启动时注册（POST `/api/workers/register`）
- 心跳维持（POST `/api/workers/{id}/heartbeat`，15s 间隔）
- 轮询 claim 任务（POST `/api/workers/{id}/claim-task`，3s 间隔）
- 本地执行 Pipeline 后上报 complete/fail
- 通过 Pipeline 回调同步上报进度事件
- 支持 draining 模式（优雅退出，不再领取新任务）
- 能力推断：声明 collector 名称时自动派生所需 session capability

## 关键约定

- **所有 API 路由**挂载在 `/api` 前缀下，页面路由 (`pages.py`) 无前缀。管理端路由统一依赖 `require_admin`。
- **前端是纯 JS SPA**，无框架。页面组件在 `src/web/src/pages/<name>/index.js`，核心模块在 `src/web/src/core/`。
- **服务层单例**通过模块级 lazy getter 函数获取（`get_task_service()` 等），不走 DI 容器。这些 getter 定义在 `app.py` 中。
- **调度器 `Scheduler`** 是全局单例，`app.py` 在 lifespan 中启动/停止它。
- **无 ORM 迁移**。SQLAlchemy 表由 `Base.metadata.create_all()` 在存储初始化时按需创建。
- **存储通过工厂获取**：所有代码通过 `get_storage()` 获取存储实例，不直接实例化具体类。存储后端由 `config/settings.yaml` 中的 `database.provider` 控制。
- **嵌入模型**：`get_embeddings()` 工厂函数（`src/services/_utils.py`）返回 DashScope Embeddings 实例。
- **采集器需登录态**：SteamDB 通过 CDP 连接已登录 Chrome（`scripts/steamdb_login.py`），七麦通过 Playwright 持久化 profile（`scripts/qimai_login.py`）。Worker 通过能力标签声明登录态资源，Scheduler 根据 `SessionRegistry` 路由任务。
- **Scheduler 执行后端**有两种：`in_process`（默认，本地执行）和 `worker_claim`（任务放入队列等待 Worker 领取）。通过 `scheduler.execution_backend` 配置切换。两种后端最终都走 `DAGExecutor`：in_process 经 `Pipeline.execute()` 委托，worker_claim 经 claim payload 的 `graph` 字段（`payload_version="2"`，旧版回退 `pipeline` 字段）。
- **DAG 持久化**：图定义存 `state_type="graph"`（key `graph:{name}`），旧 pipeline 快照 `state_type="pipeline"`。lifespan 启动时 `migrate_pipelines_to_dag` 自动把旧 pipeline 转成 graph（幂等，只转不删，原记录标 `migrated:true`）。
- **新增采集器需定义 metadata**：在 `src/core/collector_metadata.py` 的 `_COLLECTOR_METADATA` 字典中添加条目，声明 capability、session 需求、checkpoint 级别、target schema。
- **Checkpoint 恢复级别**：L0（不支持）→ L1（本地记录）→ L2（跨 Worker 续传，需 session）→ L3（幂等，任意 Worker）。

## 注意事项

- **Python 3.12+** 必需。Windows 下会自动切换到 `WindowsProactorEventLoopPolicy`（兼容 Playwright 子进程）。
- **Worker Agent 可选**：默认 Scheduler 本地执行所有任务。启动 Worker Agent 后，需要登录态的任务会自动路由到声明对应能力的 Worker。Worker 通过环境变量或命令行参数配置 `--base-url`、`--capability` 等。
- **Agent MCP 依赖**：Playwright MCP 工具需要 Node.js 和 `npx`。Windows 下 Playwright 不可用或连续失败时会自动降级并禁用浏览器工具。
- **前端有两套 JS 加载路径**：`index.html` 通过 Jinja2 条件分三路 — ① Vite dev (`VITE_DEV=1`, localhost:5173) ② Vite 构建产物 (`static/dist/`, 需 `npm run build`) ③ 静态脚本 (`static/agent.js` 等)。修改 `src/web/src/` 源码后必须 `npm run build`，否则 Vite 构建模式下不生效。
- **前端构建产物已提交到 git**（`src/web/static/dist/`），修改前端后记得 `npm run build` 再提交。
- **config/settings.yaml** 中的敏感值用 `${VAR_NAME}` 引用环境变量，不要直接写明文密钥。数据库连接通过 `DATABASE_URL` 环境变量配置。
- **采集器配置多层级解析**：`collect_batch()` 的重试/超时/并发参数按 collector 级 → global 级 fallback。
- 集成测试标记为 `@pytest.mark.integration`，会访问外部服务，CI 环境需要跳过或配置凭据。
- 部分采集器（Google Trends、Twitch、Firecrawl）需要海外网络连通性。
- `pgvector` 为可选依赖（`models.py` 中自动降级为 JSON 类型）。
- **Edit 工具修改 HTML 模板**时，可能将属性引号保存为 `\"` 字面量，导致浏览器无法识别元素 ID。修改 `index.html` 后务必用 `grep` 或 `curl` 检查渲染输出。
