"""LangChain Agent 模块 —— 自然语言驱动的数据采集助手"""

__all__ = ["AgentService"]


def __getattr__(name: str):
    if name == "AgentService":
        from src.agent.agent import AgentService

        return AgentService
    raise AttributeError(name)
