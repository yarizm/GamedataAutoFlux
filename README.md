# GamedataAutoFlux

GamedataAutoFlux 是一个面向游戏数据采集、整理和报告生成的本地工作流项目。项目提供 FastAPI WebUI，用于配置采集管线、提交任务、浏览已落库 JSON 数据、按数据源生成 Excel 报告，以及配置定时任务。

当前项目仍依赖多个外部站点和第三方接口。部分数据源需要 API Key、登录态或较长等待时间；外部站点页面结构、权限和风控策略变化时，采集结果可能为空或不完整。

## 功能范围

### WebUI

启动后访问 `http://localhost:8000`，当前页面包含：

- 仪表盘：查看任务数量、运行状态和近期任务。
- 任务管理：创建采集任务，选择 Pipeline，填写 App ID、URL、时间区间、数据分组等参数。
- Pipeline：创建或删除采集管线，可从预设模板生成。
- 数据浏览：按游戏、App ID、数据分组查看已落库 JSON；支持预览、导出、编辑、删除、更新内容、按任务名或 ID 搜索。
- 报告：选择或上传多个 JSON 数据源，选择报告模板并生成 Excel 报告；支持报告历史编辑、删除和下载。
- 定时任务：通过 cron 表达式定期提交采集任务，也可用于数据更新任务。

### 数据源

已注册的采集器包括：

| 采集器 | 说明 | 常用参数 |
| --- | --- | --- |
| `steam` | Steam 官方 API、评论摘要、新闻事件、SteamDB 补充数据、Steam 畅销榜等 | `app_id`、`time_slice`、`start_date`、`end_date` |
| `steam_discussions` | Steam Community Discussions 论坛讨论采集 | `app_id`、`start_date`、`end_date`、`include_replies` |
| `taptap` | TapTap 游戏详情、评分、评论等公开页面数据 | `taptap_app_id`、`url`、`reviews_pages`、`reviews_limit` |
| `gtrends` | Google Trends 搜索趋势 | `keyword`、`geo`、`timeframe` |
| `monitor` | 外围监控指标，目前包括 Twitch/SullyGnome 观看趋势等 | `app_id`、`days`、`twitch_name`、`siteurl` |
| `qimai` | 七麦/App Store 排名、评分、下载/收入预估等页面数据 | `qimai_app_id`、`country` |

处理器：

- `cleaner`：清洗采集结果，统一补充 `game_name`、`app_id`、`group_id`、`task_id` 等字段。
- `embedding`：将文本内容向量化，供语义检索或后续报告检索使用。

存储：

- `local`：将清洗后的 JSON 存到 `data/results`，同时维护本地索引。
- `vector`：将向量记录写入本地向量存储。未配置真实向量库时可按配置回退到本地 stub。

### 报告模板

报告页面支持从数据浏览页选择 JSON、按分组导入 JSON、手动上传 JSON，再生成 Excel 报告。

当前结构化模板包括：

- `general_game`：通用游戏模板。目标数据源包括 Steam、TapTap、Google Trends、Monitor、事件数据、Steam 社区讨论；七麦为可选数据源。
- `taptap_game`：TapTap 游戏模板。主要使用 TapTap 数据。
- `steam_game`：Steam 游戏模板。目标数据源包括 Steam、Google Trends、Monitor、事件数据、Steam 社区讨论。

涉及 Steam 在线人数、SteamDB、Google Trends、Twitch/Monitor、七麦趋势等时序数据时，报告生成器会在 Excel 中写入趋势数据附表，并按模板生成折线图。报告文本部分默认调用 Qwen 兼容接口；失败时会回退到模板化报告。

## 环境要求

- Python `>=3.12`
- Windows PowerShell 或其他可运行 Python 的终端
- Chromium/Chrome/Edge，用于 Playwright 采集动态页面
- 可选：Steam Web API Key、DashScope/Qwen API Key、Firecrawl API Key、Google Trends 可访问网络环境

## 安装

```powershell
cd C:\Users\YARIZM\PycharmProjects\GamedataAutoFlux

python -m venv .venv
.\.venv\Scripts\Activate.ps1

pip install -e .[dev,embedding]
playwright install chromium
```

如果只运行基础 WebUI，可以先安装：

```powershell
pip install -e .
playwright install chromium
```

## 配置

主配置文件为 `config/settings.yaml`。敏感信息建议通过环境变量设置，不要直接写入仓库文件。

常用环境变量：

```powershell
$env:STEAM_API_KEY="your_steam_api_key"
$env:DASHSCOPE_API_KEY="your_dashscope_api_key"
$env:FIRECRAWL_API_KEY="your_firecrawl_api_key"
$env:FIRECRAWL_COOKIE="optional_cookie_string"
$env:OPENAI_API_KEY="optional_openai_key"
$env:DEEPSEEK_API_KEY="optional_deepseek_key"
```

也可以参考 `.env.example`。当前项目不会自动加载 `.env` 文件；如果需要使用 `.env`，请在启动前自行加载到进程环境变量。

## 启动 WebUI

```powershell
.\.venv\Scripts\Activate.ps1
python -m src.web.app
```

或在安装项目脚本后运行：

```powershell
autoflux
```

默认监听地址来自 `config/settings.yaml`：

```yaml
server:
  host: "0.0.0.0"
  port: 8000
```

浏览器访问：

```text
http://localhost:8000
```

## 基本使用流程

1. 打开 WebUI。
2. 在 `Pipeline` 页面创建或选择预设 Pipeline，例如 `steam_basic`、`steam_discussions_basic`、`gtrends_basic`、`monitor_basic`、`qimai_basic`。
3. 在 `任务管理` 页面创建任务，填写目标游戏名称、App ID、时间区间和数据分组。
4. 等待任务完成。运行中的任务可在任务列表中查看状态和日志。
5. 在 `数据浏览` 页面按游戏或分组查看结果，预览原始 JSON，必要时编辑、删除或导出。
6. 在 `报告` 页面添加多个 JSON 数据源，选择模板后生成 Excel 报告。
7. 如需周期性采集，在 `定时任务` 页面配置 cron 任务。

## SteamDB 登录态采集

SteamDB 的部分图表和历史数据可能需要登录态，也可能触发 Cloudflare。项目支持通过本地 Chrome/Edge 的 CDP 端口复用人工登录后的浏览器会话。

启动登录浏览器：

```powershell
.\.venv\Scripts\Activate.ps1
python scripts\steamdb_login.py --port 9222
```

操作步骤：

1. 脚本会打开一个独立浏览器窗口，用户数据目录为 `data/steamdb_profile`。
2. 在该窗口中手动登录 SteamDB。
3. 登录完成后按回车，保持该浏览器窗口打开。
4. 再提交 Steam 采集任务，或运行 smoke 脚本验证。

验证 SteamDB CDP 采集：

```powershell
python scripts\steamdb_smoke.py --app-id 2507950 --time-slice daily_precise_90d --cdp-port 9222
```

输出 JSON 会写入 `tmp/steamdb_cdp_smoke_<app_id>.json`。摘要中会包含：

- `steamdb_signed_in`：当前 CDP 浏览器是否看起来处于登录态。
- `daily90_count`：90 天在线人数趋势点数。
- `review_history_count`：SteamDB User reviews history 解析出的点数。
- `review_history_reason`：评论历史为空时的原因说明。

注意：`data/steamdb_profile` 是独立浏览器配置。日常 Chrome 已登录 SteamDB，不代表这个 profile 已登录。

## 七麦登录态

七麦采集使用 Playwright 持久化目录 `data/qimai_profile`。如需先人工登录：

```powershell
.\.venv\Scripts\Activate.ps1
python scripts\qimai_login.py
```

登录完成后关闭脚本提示的浏览器。后续七麦采集会复用该 profile。七麦存在访问频率和权限限制，部分下载量、收入、DAU、趋势数据可能需要账号权限或页面接口返回支持。

## 常用 Pipeline 模板

WebUI 的 Pipeline 页面会从后端返回预设模板。常用模板包括：

- `steam_basic`：`steam -> cleaner -> local`
- `steam_full_report`：`steam -> cleaner -> embedding -> local -> vector`
- `steam_discussions_basic`：`steam_discussions -> cleaner -> local`
- `taptap_basic`：`taptap -> cleaner -> local`
- `gtrends_basic`：`gtrends -> cleaner -> local`
- `monitor_basic`：`monitor -> cleaner -> local`
- `qimai_basic`：`qimai -> cleaner -> local`

实际可用模板以 `/api/pipeline-templates` 返回结果为准。

## 数据文件位置

默认路径：

- SQLite 数据库：`data/autoflux.db`
- JSON 结果：`data/results`
- 调度持久化数据：`data/scheduler_tasks`
- 日志：`logs`
- 临时输出：`tmp`
- 七麦浏览器 profile：`data/qimai_profile`
- SteamDB 浏览器 profile：`data/steamdb_profile`

## API 入口

WebUI 使用的主要 API：

- `GET /api/components`：查看已注册组件。
- `GET /api/pipeline-templates`：查看预设 Pipeline。
- `GET /api/pipelines`、`POST /api/pipelines`、`DELETE /api/pipelines/{name}`：管理 Pipeline。
- `POST /api/tasks`、`GET /api/tasks`、`GET /api/tasks/{id}`：提交和查看任务。
- `GET /api/data/games`、`GET /api/data/records` 等：浏览已落库数据。
- `POST /api/reports/generate-excel`：生成 Excel 报告。
- `GET /api/reports/{id}/download`：下载报告文件。
- `POST /api/cron-jobs`、`GET /api/cron-jobs`、`DELETE /api/cron-jobs/{name}`：管理定时任务。

具体请求结构以 `src/web/routes/` 中的 Pydantic 模型为准。

## 测试

运行全部测试：

```powershell
.\.venv\Scripts\Activate.ps1
python -m pytest tests -q
```

运行指定测试文件时，替换为当前工作区实际存在的测试路径：

```powershell
python -m pytest tests\<test_file>.py -q
```

编译检查：

```powershell
python -m compileall -q src scripts tests
```

真实外部访问测试不要默认放入单元测试流程。涉及 SteamDB、七麦、TapTap、Google Trends、Firecrawl 的真实访问会受到网络、登录态、账号权限和访问频率影响。

## 开发说明

项目核心目录：

```text
src/
  collectors/      数据采集器
  processors/      清洗、向量化等处理器
  storage/         本地 JSON、SQLite、向量存储
  core/            Pipeline、Task、Scheduler、Registry
  reporting/       报告模板、数据抽取、Excel 生成、LLM 调用
  web/             FastAPI 路由、页面模板、前端静态资源
scripts/           登录态辅助脚本和 smoke 脚本
tests/             单元测试和 Web API 测试
config/            默认配置
data/              运行时数据目录
logs/              日志目录
tmp/               临时文件和测试输出
```

新增采集器的一般步骤：

1. 在 `src/collectors/` 中实现 `BaseCollector`。
2. 使用 `@registry.register("collector", "<name>")` 注册。
3. 如需 WebUI 预设，在 `src/web/routes/pipelines.py` 中添加 Pipeline 模板。
4. 如需进入结构化报告，在 `src/reporting/data_extractor.py` 中增加数据抽取逻辑，并在 `src/reporting/report_templates.py` 中声明模板依赖。
5. 增加对应测试。

## 使用限制

- SteamDB、七麦、TapTap 等页面采集依赖页面结构和登录状态，失败时应优先检查登录态、网络、权限和访问频率。
- Firecrawl 作为兜底采集时，返回内容取决于 Firecrawl 的抓取能力、配置和目标站点限制。
- Google Trends 可能出现 429 或地区网络不可达，可在配置中设置代理或降低频率。
- LLM 报告生成受模型上下文长度和接口稳定性影响；项目会在失败时保留模板化报告结果。
- 本项目默认用于本地分析流程。提交大批量任务前，应先用小时间区间或单个 App ID 验证数据源可用性。
