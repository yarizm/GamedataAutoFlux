# GamedataAutoFlux

一个面向 Steam 游戏数据采集、向量检索和报告生成的工作流项目。  
当前项目已经打通了：

- Steam 官方 API 采集
- SteamDB 增强采集
- Playwright 失败后的 Firecrawl 兜底
- Qwen Embedding
- 本地向量检索
- DeepSeek 报告生成
- WebUI 任务、Pipeline、报告、定时任务管理

## 主要功能

- 任务创建与执行
- Pipeline 编排
- Steam 数据采集与清洗
- 向量化与语义检索
- 报告生成与历史查看
- 定时任务调度

WebUI 当前主要页面包括：

- 仪表盘
- 任务管理
- Pipeline
- 报告
- 定时任务

## 目录说明

- `src/collectors/`
  数据采集器
- `src/processors/`
  数据处理器，例如 `cleaner`、`embedding`
- `src/storage/`
  本地存储和向量存储
- `src/reporting/`
  报告生成
- `src/web/`
  FastAPI 与 WebUI
- `config/settings.yaml`
  全局配置
- `tests/`
  单元测试与联调测试

## 环境要求

- Python `>= 3.12`
- Windows / macOS / Linux
- 如果需要 Playwright，请安装 Chromium

## 安装

建议先创建虚拟环境，再安装项目。

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .[dev]
playwright install chromium
```

如果你只需要基础运行，不跑测试，也可以：

```powershell
pip install -e .
```

## 配置方式

项目的密钥不再直接写在 `config/settings.yaml` 中，而是通过环境变量注入。  
当前使用到的环境变量有：

- `DASHSCOPE_API_KEY`
- `DEEPSEEK_API_KEY`
- `STEAM_API_KEY`
- `FIRECRAWL_API_KEY`

`settings.yaml` 已经支持 `${ENV_VAR}` 占位方式，启动时会自动解析。

仓库中提供了 [.env.example](C:\Users\YARIZM\PycharmProjects\GamedataAutoFlux\.env.example) 作为变量模板参考。

## PowerShell 配置示例

可以把密钥写入 Windows 用户环境变量：

```powershell
[Environment]::SetEnvironmentVariable("DASHSCOPE_API_KEY", "你的DashScopeKey", "User")
[Environment]::SetEnvironmentVariable("DEEPSEEK_API_KEY", "你的DeepSeekKey", "User")
[Environment]::SetEnvironmentVariable("STEAM_API_KEY", "你的SteamApiKey", "User")
[Environment]::SetEnvironmentVariable("FIRECRAWL_API_KEY", "你的FirecrawlKey", "User")
```

配置完成后，重启终端、IDE 或运行服务的进程，再启动项目。

临时只在当前 PowerShell 窗口生效也可以：

```powershell
$env:DASHSCOPE_API_KEY="你的DashScopeKey"
$env:DEEPSEEK_API_KEY="你的DeepSeekKey"
$env:STEAM_API_KEY="你的SteamApiKey"
$env:FIRECRAWL_API_KEY="你的FirecrawlKey"
```

## 启动项目

项目提供了命令行入口：

```powershell
autoflux
```

也可以直接启动 FastAPI 应用：

```powershell
python -m src.web.app
```

默认监听地址由 `config/settings.yaml` 控制：

- `server.host`
- `server.port`

默认访问地址通常是：

```text
http://127.0.0.1:8000
```

## WebUI 使用说明

### 1. 创建 Pipeline

进入 `Pipeline` 页面后，可以：

- 使用预设模板创建
- 手动选择 `collector / processor / storage`
- 或者直接填写 JSON 步骤配置

常见基础链路：

```text
steam -> cleaner -> local
```

带向量检索的链路：

```text
steam -> cleaner -> embedding -> vector
```

### 2. 创建任务

进入 `任务管理` 页面后，可以填写：

- 任务名称
- Pipeline
- 目标名称
- Steam App ID
- 是否跳过 SteamDB
- SteamDB 在线人数时间切片

当前 SteamDB 在线人数支持两种切片请求：

- `monthly_peak_1y`
  最近 12 个月的月度 Peak 数据
- `daily_precise_30d`
  最近 30 天的日级精确值

注意：

- 当前 `Firecrawl -> SteamDB` 的返回内容通常只能稳定提供月度表
- 如果你请求 `daily_precise_30d`，但源站内容没有精确日级点位，结果里会明确返回不可用原因，不会静默伪造数据

### 3. 生成报告

进入 `报告` 页面后，可以填写：

- 提示词
- 数据源过滤
- 模板

报告生成当前默认走 `DeepSeek`。

## 当前采集链路

### Steam 基础数据

来自 Steam 官方 API，主要包括：

- 游戏详情
- 当前在线人数
- 评论
- 成就
- 新闻

### SteamDB 增强数据

优先尝试 `Playwright`，如果遇到 Cloudflare 或页面访问失败，则自动回退到 `Firecrawl`。

当前增强数据主要包括：

- 最近 12 个月月度在线峰值
- 更新历史
- 更新时间
- 最近变更号

## 关于 SteamDB 和 Cloudflare

当前项目里：

- `Playwright` 已可正常启动
- 但访问 `steamdb.info` 时仍可能被 Cloudflare 拦截
- 被拦截后会自动切换到 `Firecrawl`

这是当前设计上的正常路径，不代表任务失败。

## 数据输出

默认本地数据会写入：

- `data/results/`
  采集结果 JSON
- `data/reports/`
  报告历史
- `data/vector_records/`
  向量数据
- `data/scheduler_tasks/`
  调度器持久化任务

本地数据库默认包括：

- `data/autoflux.db`
- `data/vector_store.db`
- `data/reports.db`
- `data/scheduler.db`




