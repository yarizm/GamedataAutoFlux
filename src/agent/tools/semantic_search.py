from typing import Type
from langchain_core.tools import BaseTool
from pydantic import BaseModel, Field
from loguru import logger
from src.services._utils import get_embeddings, extract_record_identity

from src.agent.tools.utils import _format_result, _safe_error_text


class SemanticSearchInput(BaseModel):
    query: str = Field(description="The natural language query to search for.")
    limit: int = Field(default=5, description="Maximum number of results to return.")


class SemanticSearchTool(BaseTool):
    name: str = "semantic_search"
    description: str = "通过自然语言语义检索最相关的游戏数据或评论。支持模糊查询，如'寻找包含末日生存元素的游戏'或'关于战斗系统优点的评论'。"
    args_schema: Type[BaseModel] = SemanticSearchInput

    async def _arun(self, query: str, limit: int = 5) -> str:
        from src.storage.factory import get_storage

        embeddings = get_embeddings()
        if not embeddings:
            return "[Error] Failed to initialize embeddings. Please check your configuration."

        try:
            query_vector = await embeddings.aembed_query(query)
        except Exception as e:
            return _format_result("error", f"向量化查询失败: {_safe_error_text(e)}")

        store = get_storage()
        await store.initialize()
        try:
            if not hasattr(store, "semantic_search"):
                return _format_result(
                    "error", "当前存储组件不支持 semantic_search (可能未连接到 PostgreSQL/pgvector)"
                )

            result = await store.semantic_search(query_vector=query_vector, limit=limit)

            if not result.records:
                return _format_result(
                    "empty",
                    f"未找到与 '{query}' 语义相关的记录。可能是因为尚未采集包含文本向量的数据。",
                )

            summaries = []
            for record in result.records:
                identity = extract_record_identity(record)
                summaries.append(
                    {
                        "key": record.key,
                        "source": record.source,
                        "game": identity.get("game_name", "") if identity else "",
                        "app_id": identity.get("app_id", "") if identity else "",
                        "data": record.data,
                    }
                )

            games = list({s["game"] for s in summaries if s["game"]})
            return _format_result(
                "ok",
                f"根据语义匹配 '{query}'，找到 {len(summaries)} 条最相关记录，涉及游戏: {', '.join(games[:5])}",
                summaries,
                record_count=len(summaries),
                max_data_length=15000,
            )
        except Exception as e:
            logger.error(f"Semantic search failed: {_safe_error_text(e)}")
            return _format_result("error", f"执行语义检索时发生错误: {_safe_error_text(e)}")
        finally:
            await store.close()

    def _run(self, **kwargs) -> str:
        raise NotImplementedError("Use _arun")
