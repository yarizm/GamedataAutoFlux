# GamedataAutoFlux

GamedataAutoFlux 是一个基于 Python 的游戏数据自动化采集与分析工作流。项目提供基于 FastAPI 的可视化 WebUI，支持配置数据采集管线、执行定时或手动任务、浏览并管理数据，以及生成结构化的 Excel 分析报告。

本项目主要面向本地和私有化部署。由于依赖多个外部站点和第三方接口，部分数据源需要 API Key 或浏览器登录态。如果目标站点的页面结构或风控策略发生变化，相应的采集节点可能需要调整或排查。

## 核心功能

- **可视化任务管理**：通过 WebUI 创建、查看和管理采集任务，支持实时查看运行日志。
- **多数据源集成**：
  - **Steam 生态**：Steam 官方 API、社区讨论、SteamDB 补充数据等。
  - **通用采集**：dynamic_playwright 采集器支持对任意网页进行配置驱动的 CSS 选择器或 JS 脚本提取。
- **其他平台**：TapTap 数据、Google Trends 搜索趋势、七麦数据 (Qimai)、外围监控（如 Twitch 等）。
- **模块化采集管线 (Pipeline)**：支持按需组合”采集器 -> 清洗器 -> 向量化 -> 存储”流水线。
- **多存储后端**：基于 SQLAlchemy 的统一存储层，PostgreSQL + pgvector 向量检索。
- **语义搜索**：通过向量嵌入实现自然语言语义检索，Agent 可直接用中文描述查找相关游戏数据。
- **数据管理与检索**：按游戏、App ID、数据分组等维度浏览并管理已落库的数据。
- **自动化报告生成**：整合多源数据并借助大语言模型 (LLM)，自动生成包含数据图表和分析文本的 Excel 报告。
- **定时调度机制**：内置基于 cron 表达式的定时任务系统，适用于周期性的数据监控任务。
- **告警通知**：支持钉钉 / Discord / 通用 Webhook，任务失败时自动推送通知。
- **任务可观测性**：结构化事件日志、产物 (Artifacts)、Checkpoint 快照，任务执行过程全程可追溯。
- **断点恢复**：支持从 Checkpoint 继续执行（Google Trends、Steam Discussions 等），避免中途失败后从头重跑。
- **分布式 Worker**：可选的多节点执行架构，Worker Agent 自动领取任务、匹配采集器会话（Session Registry），适合多机器/多账号并发采集。

## 安装与启动

项目支持 Docker 快速部署或本地物理环境运行。

### 方式一：Docker 部署（推荐）

如果只需运行使用环境，推荐使用 Docker 一键部署：

1. 克隆或下载本项目源码到本地。
2. 复制环境变量配置文件并填写必要的密钥（如 API Key 等）：
   ```bash
   cp .env.example .env
   ```
3. 启动容器：
   ```bash
   docker-compose up -d
   ```
4. 启动完成后，在浏览器访问 `http://localhost:8000` 即可进入 WebUI。

### 方式二：本地运行

#### 环境要求
- Python >= 3.12
- Windows PowerShell、CMD 或 Linux/macOS 终端
- Chromium/Chrome/Edge（用于 Playwright 动态网页抓取）

#### 安装步骤

1. 进入项目根目录并创建 Python 虚拟环境：
   ```powershell
   cd GamedataAutoFlux
   python -m venv .venv

   # Windows 激活虚拟环境
   .\.venv\Scripts\activate

   # Linux/macOS 激活虚拟环境
   source .venv/bin/activate
   ```
2. 安装项目依赖：
   ```powershell
   # 安装项目核心依赖及附加模块
   pip install -e .[dev]

   # 安装 Playwright 运行所需的 Chromium 浏览器
   playwright install chromium
   ```

#### 启动服务
```powershell
# 确保在已激活的虚拟环境中运行：
python -m src.web.app

# 或者直接使用快捷命令（前提是 pip install -e . 已成功安装）：
autoflux
```
启动后在浏览器访问 `http://localhost:8000`。

#### 启动 Worker Agent（可选）

Worker Agent 是分布式执行节点，适合多机器/多账号并发采集。服务端启动后，在任意可访问服务端的机器上运行：

```powershell
# 基础启动（本地测试）
python scripts/worker_agent.py --base-url http://127.0.0.1:8000

# 声明能力标签（匹配需要特定登录态或 session 的采集器）
python scripts/worker_agent.py --base-url http://192.168.1.100:8000 --capability steamdb_profile --capability qimai_session
```

Worker Agent 启动后自动注册，定期心跳，轮询领取匹配自身能力的任务。被领取的任务不再由服务端本地执行。

## 环境变量与配置

主配置文件位于 `config/settings.yaml`。为了安全，API Key 等敏感凭证建议写入 `.env` 文件或配置在系统的环境变量中。

常用环境变量说明（可参考 `.env.example`）：
- `STEAM_API_KEY`：Steam Web API 密钥，用于获取官方数据。
- `DASHSCOPE_API_KEY` / `DEEPSEEK_API_KEY` / `SENSE_API_KEY` / `OPENAI_API_KEY`：用于 Agent 或报告生成的语言模型 API 密钥（按所选 provider 填写）。
- `FIRECRAWL_API_KEY`：用于复杂网页的兜底抓取服务。
- `DATABASE_URL`：数据库连接串，默认 `postgresql+asyncpg://postgres:postgres@localhost:5432/autoflux`。

项目启动时会自动读取项目根目录的 `.env` 文件；也可以直接使用系统环境变量覆盖。

## 核心使用流程

1. **配置 Pipeline**：进入 WebUI 的 `Pipeline` 页面，点击预设模板（如 `steam_basic`、`qimai_basic`、`dynamic_playwright_basic` 等）快速生成数据管线。
2. **提交任务**：进入 `任务管理`，填写目标游戏名称、游戏 ID、时间范围和分组名等信息，并选择对应的管线进行采集。
3. **查看结果**：任务完成后，在 `数据浏览` 页面可查看、搜索落库结果，支持直接预览原始 JSON 格式。
4. **生成报告**：进入 `报告` 页面，勾选刚刚采集的数据记录，选择相应的分析模板，即可生成并下载包含图表与文本的 Excel 文件。
5. **周期任务**：针对需要长期追踪的数据，可在 `定时任务` 页面配置基于 cron 的自动化调度规则。
6. **系统监控**：`系统` 页面展示 Worker 在线状态、能力标签、当前任务；可查看采集器 Session 清单（账号绑定、登录态健康度、租约状态），并对失联 Worker 执行 reconcile 清理。
7. **任务恢复**：支持 L1 Checkpoint 的采集器（如 Google Trends、Steam Discussions）在中断后可从中断位置继续，避免重复采集。

## 高级采集：维护登录态

针对反爬严格或必须登录才能查看数据的站点（如 SteamDB、七麦数据），项目采用本地浏览器复用用户会话（Profile）的解决方案。

### 1. SteamDB 登录态采集
SteamDB 有较强的反爬与 Cloudflare 限制，项目支持通过 CDP 端口连接人工已登录的 Chrome 实例。

**方式一：通过 WebUI 一键唤起（推荐）**
1. 进入 WebUI 的 `系统监控` 或 `配置` 面板的诊断项（如提示未检测到 SteamDB 浏览器运行）。
2. 点击 **一键启动浏览器** 按钮。
3. 在弹出的独立浏览器窗口中，手动登录 SteamDB 并完成可能出现的验证码。
4. **登录完成后不要关闭该浏览器窗口**。

**方式二：通过命令行手动启动**
```powershell
# 启动本地登录专用浏览器窗口
python scripts\steamdb_login.py --port 9222
```
操作完成后，在 WebUI 提交包含 SteamDB 节点的采集任务，后台会自动连接该浏览器并进行数据抓取。

### 2. 七麦数据 (Qimai) 采集
七麦采集基于 Playwright 的持久化目录运行，仅需先手动登录一次：
```powershell
# 启动七麦登录辅助脚本
python scripts\qimai_login.py
```
操作说明：在弹出的窗口中扫码或输入密码完成登录，页面加载完毕后直接关闭浏览器即可。后续自动化任务将默认复用此登录状态。

## 数据存储

项目使用 SQLAlchemy 作为 ORM 层，基于 PostgreSQL + pgvector 向量检索。

主要数据目录：
- `logs/`：系统与各采集任务的运行日志。
- `tmp/`：生成的 Excel 报告及各类临时中转文件。
- `data/steamdb_profile/` & `data/qimai_profile/`：用于高级采集的浏览器独立用户配置目录。

## 注意事项与限制

- **风控策略变化**：外部站点的页面结构和反爬策略随时可能变化。若遇到抓取失败、超时或返回空数据的情况，请优先排查本地登录态是否失效、访问频率是否过快（例如 Google Trends 返回 429 错误）或网络环境问题。
- **大批量抓取建议**：由于依赖单点请求及本地性能，提交大批量或时间跨度极长的时间段前，强烈建议先用单个游戏 ID 进行小范围测试，确认链路正常。
- **网络连通性**：部分数据源（如 Google Trends、Twitch、Firecrawl）要求网络具备海外连通能力，如果在没有代理的国内直连环境下运行，会导致这些站点的采集任务直接超时或失败。
- **模型生成报告**：报告中文本的生成质量受限于所用大模型的能力与上下文长度。若调用大模型接口超时或发生异常，报告系统会自动回退到仅包含基础图表和固定版式的离线模板。

## AI Agent 对话助手

WebUI 内置基于 LangChain 的 AI Agent，支持自然语言驱动的数据采集与分析操作。

### 核心能力

| 功能 | 说明 |
|------|------|
| 多会话管理 | 支持创建/切换/重命名/删除对话会话，消息按会话分层隔离 |
| LLM 动态切换 | WebUI 中可视化管理 provider 和模型配置，运行时热切换 |
| Playwright 浏览器工具 | 通过 MCP (Model Context Protocol) 集成 Playwright，Agent 可自主打开网页、分析 DOM、执行 JS 提取脚本，并自动创建动态采集 Pipeline |
| SSE 流式中断 | 支持随时停止生成中的回复，中断后对话历史自动保存 |
| 对话历史持久化 | 工具调用卡片、思考过程、执行结果完整保存到 localStorage，页面刷新后完整恢复 |
| 任务进度追踪 | Agent 创建的任务在对话中实时显示进度卡片 |
| 分段输出 | LLM 输出按执行流程分段展示，每次工具调用的前后文字独立成段 |
| 思考过程 | 可折叠抽屉展示推理过程，支持 DeepSeek-R1/Qwen-QwQ 的原生 reasoning_content，普通模型展示工具决策描述 |

### 可用工具

Agent 可调用的工具包括：任务管理（创建/查看/取消）、数据浏览与语义搜索、Pipeline 管理（含动态 Playwright Pipeline 创建）、定时任务管理、报告生成与查看、Steam App ID 解析、游戏标识符自动发现。语义搜索工具通过向量嵌入实现自然语言检索，无需精确关键词即可找到相关游戏数据。Playwright MCP 工具（`browser_navigate`、`browser_snapshot`、`browser_evaluate` 等）使 Agent 能够直接操作浏览器进行动态网页探索和数据提取。

Agent 通过 MCP 协议集成 Playwright 浏览器，可处理系统无内置采集器的任意网页。运行 Agent 的环境需安装 Node.js 和 `npx`（Playwright MCP Server 通过 `npx -y @playwright/mcp` 启动）。Windows 下 Playwright 不可用或连续失败时会自动降级并禁用浏览器工具。

## 许可证 (License)

本项目采用 [MIT License](LICENSE) 开源许可证。

## 免责声明 (Disclaimer)

本项目仅供技术研究与教育目的使用。使用者通过本框架爬取、解析、存储的任何第三方网站（如 SteamDB, Google 等）数据，其版权归属原网站所有。使用者需自行承担因高并发抓取、绕过反爬机制等行为引发的 IP 封禁、账号封停或法律纠纷，项目作者不承担任何连带责任。
