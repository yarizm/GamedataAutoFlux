# 官方插件发布流程

本文描述 GamedataAutoFlux 第一方采集插件的版本、构建和目录发布约定。插件包与核心包独立发布；部署者只安装自己需要的能力。

## 发布前约束

每个公开插件必须同时满足：

- `plugins/<name>/pyproject.toml` 中包含名称、版本、MIT 许可证、作者和项目链接；
- 声明 `gamedata_autoflux.plugins` entry point，名称与官方目录 ID 的末段一致；
- `requires-python` 与依赖中的 `gamedata-autoflux` 版本范围准确；
- `src/plugin_manager/catalog.json` 中的 distribution、版本、目录、采集器和运行能力与包一致；
- 浏览器插件声明 `playwright-chromium`，不得在安装请求中临时安装系统浏览器；
- 内部共享包通过 `internal_dependencies` 随依赖它的官方插件一起构建。
- `PluginSpec` 声明与模块实际注册项完全一致，所有 DAG 节点、模板、probe、validator 和
  identifier resolver 均通过运行时同款契约校验。

运行只读校验：

```bash
python scripts/check_plugin_release.py
```

CI 会执行同一条命令。该命令会在独立进程加载所有插件模块并逐项验证能力；目录、包元数据、
实际组件或回调契约任一不一致时发布应停止。

## 构建候选版本

使用一个新的输出目录构建全部官方插件和内部共享包：

```bash
python scripts/check_plugin_release.py --build-dir dist/plugins
```

命令使用当前 Python 解释器逐包执行无依赖 wheel 构建，并生成 `plugin-release-manifest.json`。清单记录每个 wheel 的 distribution、版本、文件名、大小和 SHA-256。发布前应在干净环境中复跑该命令，并保留清单作为制品。

## 版本与目录更新顺序

1. 修改插件代码和测试。
2. 依据兼容性影响更新插件版本；破坏性变更必须提升主版本。
3. 同步更新官方目录中的 `version`、兼容范围和运行能力。
4. 运行发布校验、插件测试、全量后端测试和前端构建。
5. 构建 wheel，在 core-only 环境从插件中心安装并重启验证。
6. 发布 wheel 与 SHA-256 清单；随后发布目录更新。

目录不得先指向尚未公开的制品。当前仓库使用 bundled catalog，由服务端从同仓库源码构建官方 wheel；迁移到远程目录时必须把精确下载 URL 和 SHA-256 纳入签名目录，且只允许 HTTPS 受信主机。

## 回滚

发现候选版本无法激活时，不修改当前 generation。修复目录条目或重新发布新版本；不要复用已经发布过的版本号覆盖 wheel。部署端可在插件中心切回保留的上一 generation，然后重启服务。

## 安全边界

wheel 在主进程中只读取 ZIP 清单和元数据。插件导入在隔离子进程中做 smoke test，但这不是安全沙箱；本阶段仅允许受信任的第一方或经部署者明确确认的本地代码。第三方开放生态应先引入签名、发布者验证和独立 Worker 隔离。
