# ⚡ GamedataAutoFlux

**全栈式自动化游戏数据监测、智能分析与可视化报告生成平台**

GamedataAutoFlux 是一个为游戏行业分析师、运营人员和开发者打造的自动化工作流引擎。它能够跨平台追踪 Steam、TapTap 及全球搜索热度，利用 AI 大模型进行语义理解与舆情分析，最终产出包含动态图表的高质量分析报告。

---

## ✨ 核心亮点

### 🔍 全方位数据采集 (Multi-Source Collection)
- **Steam 深度追踪**：
    - **多维度指标**：采集在线人数、24 小时峰值、历史记录。
    - **评论挖掘**：自动抓取近期玩家评论，支持按语言过滤。
    - **情报兜底**：集成 SteamDB 增强抓取，当官方接口受限时自动通过自动化浏览器补充数据。
- **TapTap 舆情监控**：
    - 基于 Playwright 的深度爬取，获取移动端玩家的真实反馈与评分走势。
- **Google Trends 趋势分析**：
    - 同步全球搜索热度数据，分析游戏在不同地域的关注度趋势。

### 🧠 智能数据流处理 (Intelligent Data Pipeline)
- **深度数据“瘦身”**：内置 `DataCleaner` 处理器，在数据入库前自动剔除冗余的 HTML、Base64 和无效脚本，存储效率提升 90% 以上。
- **语义向量化**：集成 Qwen Embedding 引擎，将琐碎的玩家评论转化为高维向量，实现基于语义而非关键词的精准检索。
- **AI 智能报告**：支持 DeepSeek、OpenAI 等主流 LLM，针对采集到的原始数据，自动总结“版本更新影响”、“核心痛点”、“市场竞争力”等核心维度。

### 📊 专业级可视化报告 (Professional Reporting)
- **Excel 自动报表**：生成的 `.xlsx` 文件不仅包含原始清单，还自动嵌入了各类汇总统计图表，直接用于周报/月报汇报。
- **Web 实时仪表盘**：
    - **动态图表**：利用 ECharts 展示系统任务分布与执行趋势。
    - **实时推送**：基于 WebSocket 技术，任务执行进度与底层日志秒级同步，告别传统页面的死板刷新。

### 📅 工业级任务调度 (Enterprise Scheduling)
- **灵活 Pipeline 编排**：支持自由组合采集、处理和存储步骤，适配不同的业务场景。
- **自动化 Cron 引擎**：支持标准的 Cron 表达式，轻松实现“每日凌晨自动采集数据并发送 AI 总结”。

---

## 🚀 快速上手

### 1. 环境准备
项目基于 Python 3.12+ 开发。

```bash
# 克隆项目并进入目录
git clone https://github.com/your-repo/GamedataAutoFlux.git
cd GamedataAutoFlux

# 安装核心依赖
pip install -e .

# 安装自动化浏览器 (用于处理动态网页采集)
playwright install chromium
```

### 2. 环境配置 (Windows PowerShell 示例)
项目使用环境变量管理敏感信息。你可以参考 `.env.example` 进行设置：

**推荐：永久设置环境变量 (执行一次即可)**
```powershell
# 基础 AI 与数据接口配置
[Environment]::SetEnvironmentVariable("STEAM_API_KEY", "你的SteamApiKey", "User")
[Environment]::SetEnvironmentVariable("DEEPSEEK_API_KEY", "你的DeepSeekKey", "User")
[Environment]::SetEnvironmentVariable("DASHSCOPE_API_KEY", "你的DashScopeKey", "User")
[Environment]::SetEnvironmentVariable("FIRECRAWL_API_KEY", "你的FirecrawlKey", "User")
```

**临时在当前窗口设置：**
```powershell
$env:STEAM_API_KEY="你的Key"
$env:DEEPSEEK_API_KEY="你的Key"
```

### 3. 启动项目
```bash
python -m src.web.app
```
服务启动后，在浏览器访问：`http://localhost:8000`

---

## 🛠️ 控制台使用指南

### 1. 配置数据管线 (Pipeline)
在 **Pipeline** 页面，你可以看到“数据如何流转”。
- **Collector**: 选择数据源（Steam/TapTap/GTrends）。
- **Processor**: 启用 `cleaner` 进行瘦身，启用 `embedding` 准备 AI 检索。
- **Storage**: 决定数据是存入 SQLite (`local`) 还是向量数据库 (`vector`)。

### 2. 执行分析任务
在 **任务管理** 页面点击“+ 创建任务”：
- 输入游戏名称、AppID 或 URL。
- 勾选 **“自动生成报告”**，系统会在抓取完成后自动调动 AI 进行分析。
- 任务执行期间，你可以点击“日志”查看实时的抓取步进。

### 3. 管理分析报告
在 **报告** 页面：
- 查看 AI 整理的深度总结摘要。
- 点击 **“下载 EXCEL 报告”** 导出包含可视化统计图表的专业分析文件。

---

## 📂 项目结构概览 (核心逻辑)
- `src/collectors/` - 跨平台数据采集插件集
- `src/processors/` - 数据清洗、文本截断、向量化引擎
- `src/storage/` - SQLite 与向量数据库适配层
- `src/reporting/` - Excel 渲染器与 LLM 提示词模板
- `src/web/` - FastAPI 服务端与现代化响应式前端

---

© 2026 GamedataAutoFlux ⚡
