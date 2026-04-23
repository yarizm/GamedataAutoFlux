# GamedataAutoFlux

一个面向游戏数据采集、向量检索和报告生成的工作流项目。

## 配置方式

项目的密钥通过环境变量注入。  
当前使用到的环境变量有：

- `DASHSCOPE_API_KEY`
- `DEEPSEEK_API_KEY`
- `STEAM_API_KEY`
- `FIRECRAWL_API_KEY`


## PowerShell 配置示例

可以把密钥写入 Windows 用户环境变量：

```powershell
[Environment]::SetEnvironmentVariable("DASHSCOPE_API_KEY", "你的DashScopeKey", "User")
[Environment]::SetEnvironmentVariable("DEEPSEEK_API_KEY", "你的DeepSeekKey", "User")
[Environment]::SetEnvironmentVariable("STEAM_API_KEY", "你的SteamApiKey", "User")
[Environment]::SetEnvironmentVariable("FIRECRAWL_API_KEY", "你的FirecrawlKey", "User")
```

配置完成后，重启终端、IDE 或运行服务的进程，再启动项目。

## 示例文件

仓库中提供了 [.env.example]作为变量模板参考。
