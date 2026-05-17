# GamedataAutoFlux

GamedataAutoFlux 是一个基于 Python 的游戏数据自动化采集与分析工作流。项目提供基于 FastAPI 的可视化 WebUI，支持配置数据采集管线、执行定时或手动任务、浏览并管理数据，以及生成结构化的 Excel 分析报告。

本项目主要面向本地和私有化部署。由于依赖多个外部站点和第三方接口，部分数据源需要 API Key 或浏览器登录态。如果目标站点的页面结构或风控策略发生变化，相应的采集节点可能需要调整或排查。

## 核心功能

- **可视化任务管理**：通过 WebUI 创建、查看和管理采集任务，支持实时查看运行日志。
- **多数据源集成**：
  - **Steam 生态**：Steam 官方 API、社区讨论、SteamDB 补充数据等。
  - **其他平台**：TapTap 数据、Google Trends 搜索趋势、七麦数据 (Qimai)、外围监控（如 Twitch 等）。
- **模块化采集管线 (Pipeline)**：支持按需组合“采集器 -> 清洗器 -> 存储/向量化”流水线。
- **数据管理与检索**：按游戏、App ID、数据分组等维度浏览并管理已落库的 JSON 数据。
- **自动化报告生成**：整合多源数据并借助大语言模型 (LLM)，自动生成包含数据图表和分析文本的 Excel 报告。
- **定时调度机制**：内置基于 cron 表达式的定时任务系统，适用于周期性的数据监控任务。

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
   pip install -e .[dev,embedding]

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

## 环境变量与配置

主配置文件位于 `config/settings.yaml`。为了安全，API Key 等敏感凭证建议写入 `.env` 文件或配置在系统的环境变量中。

常用环境变量说明（可参考 `.env.example`）：
- `STEAM_API_KEY`：Steam Web API 密钥，用于获取官方数据。
- `DASHSCOPE_API_KEY` / `DEEPSEEK_API_KEY` / `SENSE_API_KEY` / `OPENAI_API_KEY`：用于 Agent 或报告生成的语言模型 API 密钥（按所选 provider 填写）。
- `FIRECRAWL_API_KEY`：用于复杂网页的兜底抓取服务。

项目启动时会自动读取项目根目录的 `.env` 文件；也可以直接使用系统环境变量覆盖。

## 核心使用流程

1. **配置 Pipeline**：进入 WebUI 的 `Pipeline` 页面，点击预设模板（如 `steam_basic`、`qimai_basic` 等）快速生成数据管线。
2. **提交任务**：进入 `任务管理`，填写目标游戏名称、游戏 ID、时间范围和分组名等信息，并选择对应的管线进行采集。
3. **查看结果**：任务完成后，在 `数据浏览` 页面可查看、搜索落库结果，支持直接预览原始 JSON 格式。
4. **生成报告**：进入 `报告` 页面，勾选刚刚采集的数据记录，选择相应的分析模板，即可生成并下载包含图表与文本的 Excel 文件。
5. **周期任务**：针对需要长期追踪的数据，可在 `定时任务` 页面配置基于 cron 的自动化调度规则。

## 高级采集：维护登录态

针对反爬严格或必须登录才能查看数据的站点（如 SteamDB、七麦数据），项目采用本地浏览器复用用户会话（Profile）的解决方案。

### 1. SteamDB 登录态采集
SteamDB 有较强的反爬与 Cloudflare 限制，项目支持通过 CDP 端口连接人工已登录的 Chrome 实例。
```powershell
# 启动本地登录专用浏览器窗口
python scripts\steamdb_login.py --port 9222
```
操作说明：
1. 脚本会打开一个独立浏览器窗口，请手动登录 SteamDB 并完成可能出现的验证码。
2. **登录完成后不要关闭该浏览器窗口**。
3. 此时在 WebUI 提交包含 SteamDB 节点的采集任务，后台会自动连接该浏览器并进行数据抓取。

### 2. 七麦数据 (Qimai) 采集
七麦采集基于 Playwright 的持久化目录运行，仅需先手动登录一次：
```powershell
# 启动七麦登录辅助脚本
python scripts\qimai_login.py
```
操作说明：在弹出的窗口中扫码或输入密码完成登录，页面加载完毕后直接关闭浏览器即可。后续自动化任务将默认复用此登录状态。

## 数据存储目录

默认情况下的主要存储路径说明：
- `data/autoflux.db`：SQLite 本地数据库（存储系统任务状态、定时配置等元数据）。
- `data/results/`：清洗后生成的结构化 JSON 数据目录。
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
| 多会话管理 | 支持创建/切换/删除对话会话，消息按会话隔离 |
| LLM 动态切换 | WebUI 中可视化管理 provider 和模型配置，运行时热切换 |
| 任务进度追踪 | Agent 创建的任务在对话中实时显示进度卡片 |
| 分段输出 | LLM 输出按执行流程分段展示，每次工具调用的前后文字独立成段 |
| 思考过程 | 可折叠抽屉展示推理过程，支持 DeepSeek-R1/Qwen-QwQ 的原生 reasoning_content，普通模型展示工具决策描述 |

### 可用工具

Agent 可调用的工具包括：任务管理（创建/查看/取消）、数据浏览与搜索、Pipeline 管理、定时任务管理、报告生成与查看、Steam App ID 解析。

### 报告生成修复（2026-05-08）

- **数据过滤**：`generate_report` 工具现在从 prompt 中提取游戏名关键词并过滤记录，不再将所有数据写入报告。关键词提取支持中英文，双向子串匹配确保 "三角洲行动数据分析" 能匹配到 "三角洲行动"。
- **报告内容可见**：`generate_report` 返回值直接附带报告正文（上限 4000 字符），新增 `get_report_content` 工具可按 ID 获取完整内容。
- **Excel 导出**：修复嵌套 dict（如价格数据 `{"currency":"GBP","initial":3999,...}`）导致 `Cannot convert dict to Excel` 错误，自动序列化为 JSON 字符串写入单元格。

## 许可证 (License)

本项目采用 [MIT License](LICENSE) 开源许可证。
