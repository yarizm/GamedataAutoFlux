"""
插件注册中心

提供装饰器注册和工厂方法获取组件（采集器、处理器、存储）。
支持自动发现和按名称实例化。

使用示例:
    @registry.register("collector", "steam")
    class SteamCollector(BaseCollector):
        ...

    # 获取实例
    collector_cls = registry.get("collector", "steam")
    collector = collector_cls(config)
"""

from __future__ import annotations

from typing import Any, Type

from loguru import logger


class ComponentRegistry:
    """
    组件注册中心（单例）。

    维护 {component_type: {name: class}} 的映射表，
    通过装饰器自动注册，通过 get() 工厂方法获取。
    """

    _instance: ComponentRegistry | None = None

    def __new__(cls) -> ComponentRegistry:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._registry = {}
        return cls._instance

    def register(self, component_type: str, name: str):
        """
        装饰器：将类注册到指定类型和名称下。

        Args:
            component_type: 组件类型，如 "collector", "processor", "storage"
            name: 组件名称，如 "steam", "cleaner", "sqlite"

        Returns:
            装饰器函数

        Example:
            @registry.register("collector", "steam")
            class SteamCollector(BaseCollector):
                ...
        """
        def decorator(cls_: Type) -> Type:
            if component_type not in self._registry:
                self._registry[component_type] = {}

            if name in self._registry[component_type]:
                logger.warning(
                    f"组件重复注册: [{component_type}] {name}，"
                    f"原 {self._registry[component_type][name].__name__} "
                    f"→ 新 {cls_.__name__}"
                )

            self._registry[component_type][name] = cls_
            logger.debug(f"组件已注册: [{component_type}] {name} → {cls_.__name__}")
            return cls_

        return decorator

    def get(self, component_type: str, name: str) -> Type:
        """
        获取已注册组件的类。

        Args:
            component_type: 组件类型
            name: 组件名称

        Returns:
            注册的类

        Raises:
            KeyError: 组件未注册
        """
        if component_type not in self._registry:
            raise KeyError(
                f"未知的组件类型: '{component_type}'，"
                f"可用类型: {list(self._registry.keys())}"
            )
        if name not in self._registry[component_type]:
            raise KeyError(
                f"未注册的 {component_type}: '{name}'，"
                f"可用组件: {list(self._registry[component_type].keys())}"
            )
        return self._registry[component_type][name]

    def create(self, component_type: str, name: str, **kwargs: Any) -> Any:
        """
        获取并实例化组件。

        Args:
            component_type: 组件类型
            name: 组件名称
            **kwargs: 传递给构造函数的参数

        Returns:
            组件实例
        """
        cls_ = self.get(component_type, name)
        return cls_(**kwargs)

    def list_components(self, component_type: str | None = None) -> dict[str, list[str]]:
        """
        列出已注册的组件。

        Args:
            component_type: 指定类型，None 则返回所有

        Returns:
            {type: [name1, name2, ...]} 映射
        """
        if component_type:
            names = list(self._registry.get(component_type, {}).keys())
            return {component_type: names}
        return {t: list(names.keys()) for t, names in self._registry.items()}

    def clear(self) -> None:
        """清除所有注册（测试用）"""
        self._registry.clear()


# 全局注册中心实例
registry = ComponentRegistry()
