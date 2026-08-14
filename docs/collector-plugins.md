# 采集器插件

GamedataAutoFlux 核心包只提供任务、DAG、调度、存储以及 `BaseCollector` 协议，
不再包含任何平台采集实现。每个平台插件都是独立的 Python distribution，通过
`gamedata_autoflux.plugins` entry point 在启动时注册自己的采集器、元数据、任务
预检规则、深度探测和执行方案（Pipeline）模板。

## 按需安装

先安装核心，再选择需要的平台：

```bash
pip install .

# 示例：只提供 YouTube 采集
pip install ./plugins/youtube

# 示例：Steam 与 Google Trends
pip install ./plugins/steam ./plugins/gtrends
```

官网和 Monitor 插件共享 Smart Web 辅助包。本仓库本地安装时需要同时指定；插件
发布到包索引后，pip 会根据依赖自动安装它：

```bash
pip install ./plugins/smart_web ./plugins/official_site
pip install ./plugins/smart_web ./plugins/monitor
```

浏览器型插件安装后还需要浏览器运行时：

```bash
playwright install chromium
```

## 插件清单

| 目录 | distribution | 注册的采集器 | 主要额外依赖 |
|---|---|---|---|
| `plugins/steam` | `autoflux-plugin-steam` | `steam`, `steam_discussions` | Playwright, Firecrawl |
| `plugins/taptap` | `autoflux-plugin-taptap` | `taptap` | Playwright, Firecrawl |
| `plugins/gtrends` | `autoflux-plugin-gtrends` | `gtrends` | pytrends, Firecrawl |
| `plugins/qimai` | `autoflux-plugin-qimai` | `qimai` | Playwright |
| `plugins/youtube` | `autoflux-plugin-youtube` | `youtube_profiles`, `youtube_comments` | httpx |
| `plugins/official_site` | `autoflux-plugin-official-site` | `official_site` | Smart Web, Playwright |
| `plugins/monitor` | `autoflux-plugin-monitor` | `monitor` | Smart Web |
| `plugins/dynamic_playwright` | `autoflux-plugin-dynamic-playwright` | `dynamic_playwright` | Playwright |

安装或卸载插件后重启 Autoflux。`GET /api/plugins` 返回每个已发现插件的激活状态和
逐项验证过的能力清单。`GET /api/components/metadata` 返回已激活组件、采集器元数据和
`dag_nodes`；DAG 节点库只消费这份运行时目录，因此新插件成功激活后，其采集器节点会
自动出现在 DAG 编排中。单个插件导入或能力校验失败会被事务回滚并记入诊断，不会影响
其他插件启动。

## 插件身份与运行时约定

Autoflux 将插件身份、安装来源和运行时元数据分开管理。插件加载时集中收集并验证所有
贡献项；验证失败的插件不会激活，但会保留诊断信息。插件中心统一展示安装状态、运行状态
和已声明的贡献项。托管安装使用版本锁定 wheel、不可变 generation 和隔离子进程验证。

本项目的唯一运行时身份来源是 distribution 中
`gamedata_autoflux.plugins` entry point 返回的 `PluginSpec`。目录 ID、展示名或源码目录名
都不能替代这个身份。官方目录中的 distribution、版本、collector 列表必须与实际
`PluginSpec` 完全一致。

## Docker

Docker 镜像默认是 core-only，不安装 Chromium。构建时通过空格分隔的平台目录名：

```bash
docker build --build-arg AUTOFLUX_PLUGINS="youtube steam" -t autoflux:custom .
```

Compose 可在 `.env` 中配置：

```dotenv
AUTOFLUX_PLUGINS=youtube steam
```

选择 `steam`、`taptap`、`qimai`、`official_site` 或 `dynamic_playwright` 时，镜像
才会安装 Playwright Chromium 与系统依赖。

## 开发自定义插件

插件入口必须返回 `src.core.plugin_system.PluginSpec`：

```toml
[project.entry-points."gamedata_autoflux.plugins"]
my-platform = "my_autoflux_plugin:plugin"
```

```python
from src.core.collector_metadata import (
    CollectorMetadata,
    CollectorTargetField,
    CollectorTargetSchema,
)
from src.core.dag_nodes import DagOutputField
from src.core.plugin_system import PluginSpec

plugin = PluginSpec(
    name="my-autoflux-plugin",
    version="0.1.0",
    modules=("my_autoflux_plugin.collector",),
    collectors=("my_platform",),
    metadata=(
        CollectorMetadata(
            collector_id="my_platform",
            display_name="My Platform",
            description="Collect public records from My Platform.",
            target_schema=CollectorTargetSchema(
                target_type="article",
                fields=[
                    CollectorTargetField(
                        key="name",
                        location="name",
                        label="目标名称",
                        description="用于在任务和数据结果中识别该目标。",
                        required=True,
                        placeholder="Example feed",
                    ),
                    CollectorTargetField(
                        key="feed_url",
                        label="Feed URL",
                        description="要采集的公开 Feed 地址。",
                        input_type="url",
                        required=True,
                        placeholder="https://example.com/feed",
                    ),
                    CollectorTargetField(
                        key="limit",
                        label="最大记录数",
                        description="本次任务最多读取多少条记录。",
                        input_type="number",
                        default=100,
                        minimum=1,
                    ),
                ],
                default_params={"limit": 100},
            ),
            config_schema={
                "type": "object",
                "properties": {
                    "request_timeout": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 30,
                        "title": "请求超时（秒）",
                        "description": "所有任务共用的固定节点配置。",
                    }
                },
            },
            output_fields=[
                DagOutputField(key="record_id", label="Record ID"),
                DagOutputField(key="title", label="Title"),
            ],
        ),
    ),
)
```

`modules` 中的采集器使用原有装饰器注册：

```python
from src.collectors.base import BaseCollector
from src.core.registry import registry

@registry.register("collector", "my_platform")
class MyCollector(BaseCollector):
    ...
```

### 执行方案、任务与 DAG 的职责

WebUI 将三种概念统一为一条操作链：

1. **执行方案（Pipeline）**描述可复用的“采集 → 处理 → 存储”步骤，本身不会运行；
2. **任务**选择一个执行方案，并填写这一次运行的目标参数；
3. **DAG**是执行方案的可视化高级编辑方式，用于分支、多采集器串联和上游字段映射。

插件必须把“每次任务变化的输入”和“节点固定配置”分开声明：

- `CollectorTargetSchema.fields` 生成任务表单。`location="name"` 写入
  `target.name`，默认的 `location="params"` 写入 `target.params`；
- `input_type` 支持 `text`、`url`、`number`、`date`、`boolean`、`select`、
  `textarea` 和 `textarea_lines`；批量目标可用 `multiple=True` 或
  `textarea_lines`；
- `label`、`description`、`required`、`placeholder`、`default` 和数值边界
  会直接用于操作提示与前端控件；
- `CollectorMetadata.config_schema` 生成 DAG 节点的固定配置表单，复杂对象仍可在
  高级 JSON 中编辑；
- `required_fields` 和 `rules` 继续负责服务端任务预检，前端表单不能替代运行时校验。

这样安装新插件后，无需修改核心页面：新采集器会进入可用执行方案与 DAG 节点库，任务向导
也会自动显示插件声明的目标字段。

### DAG 随插件生成

每个 `collectors`、`processors` 或 `storages` 声明都必须对应一次真实的组件注册。激活
成功后，核心会为这些组件自动生成常规 DAG 节点：collector 为可选 `records` 输入和必需
`records` 输出，processor 为必需输入/输出，storage 为必需输入。collector 节点的展示名、
配置 schema 和记录字段来自 `CollectorMetadata`。

需要自定义端口时，可在 `PluginSpec.dag_nodes` 中提供 `DagNodeDefinition`。显式节点只能
引用本插件声明的组件；端口名、配置 schema 和输出字段在安装阶段校验。DAG 保存接口也会
拒绝来自未安装/未激活插件的组件和未声明端口。

### 激活时的强制校验

插件贡献可以不出现在任何现有 DAG 中，但不能跳过验证。构建 generation、发布校验和服务
启动使用同一套契约检查：

- `modules` 全部可导入，插件 API 版本受支持；
- 实际注册的 collector/processor/storage 与 `PluginSpec` 声明完全相等，不允许隐藏组件；
- 每个组件继承对应基类、不是抽象类，且实现位于已声明模块；
- 每个 collector 恰好有一份有效元数据，`display_name` 不能为空；节点优先使用
  `CollectorMetadata.description`，旧插件缺失时回退到 `PluginSpec.description`，
  两者都为空则拒绝激活，并为每个组件生成一份有效 DAG 节点定义；
- Pipeline 模板中的每个步骤都能解析到当前组件，且 collector 不能偷用其他插件的能力；
- probe、配置 validator 和 identifier resolver 的 owner 必须等于插件身份，目标 collector
  必须属于当前插件，回调签名和返回契约必须有效；
- 任一项失败时，组件、元数据、节点、模板及回调注册整体回滚，插件状态标记为 `failed`。

插件还可以按需注册核心会自动编排的扩展契约：

- `CollectorMetadata.target_schema`：任务表单字段、默认目标参数与预检规则；每个公开字段
  必须提供稳定 `key`、可读 `label` 和说明用途的 `description`，`select` 还必须提供选项。
- `CollectorMetadata.config_schema` / `output_fields`：DAG 属性编辑与下游字段映射。
- `CollectorMetadata.credential_requirements`：配置值、密钥列表和 Python 依赖检查。
- `CollectorMetadata.session_accounts` / `session_checks`：本地 Profile、托管状态文件、CDP/HTTP 端点和 Worker 会话能力。
- `register_collector_config_validator(...)`：插件专属采集器配置校验；Pipeline API、DAG 和 Agent 共用同一套结果。
- `register_identifier_resolver(...)`：可选的名称解析、标识符验证和任务目标参数自动回填；核心不维护平台白名单。

这些注册都包含在插件激活事务中。若任一能力导入或验证失败，采集器、元数据、DAG 节点、
模板、探针、校验器和标识符解析器会一起回滚。

### 插件冲突边界

同一 generation 内的插件共享 Python 运行环境，但不能静默覆盖彼此：

- 插件名称全局唯一；相同名称和版本若契约内容不一致，会以
  `PLUGIN_IDENTITY_CONFLICT` 拒绝激活；
- collector、processor、storage、DAG 节点、元数据、Pipeline 模板、probe、validator
  和 identifier resolver 都按 owner 校验，跨插件占用同一逻辑 ID 会失败并整体回滚；
- 构建 generation 前会比较 wheel 的最终安装路径；两个 wheel 写入同一 Python 文件、
  重复 distribution，或尝试提供平台保留的 `src`、`.pth`、`sitecustomize.py` 路径时，
  会以 `PLUGIN_WHEEL_CONFLICT` 拒绝安装；
- 所有插件的外部 Python 依赖会交给同一次 pip 解析；无法同时满足的版本约束会以
  `PLUGIN_DEPENDENCY_INSTALL_FAILED` 终止 generation，不会切换当前运行版本。

`AUTOFLUX_PLUGIN_MODULES` 只用于源码开发和测试，可显式列出尚未安装的插件模块；
生产部署应使用 distribution entry point，避免隐式扫描源码目录。
