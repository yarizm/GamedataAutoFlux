"""DAG 内置逻辑节点：条件分支（condition）、过滤（filter）、字段转换（transform）。

对标 Dify 的 IF/ELSE、List Operator、Template Transform 与 n8n 的 IF / Filter /
Edit Fields。数据模型沿用本项目"records 沿边流动"的方式（与 n8n 的 items 一致）：

- 节点只处理自己 ``records`` 入端口收到的记录；
- 记录类型原样保留（CollectResult / ProcessOutput），下游 processor / storage
  不需要知道中间插了逻辑节点。

字段路径写法：``data.title``、``target.name``、``metadata.lang``、``success``、
``error``、``error_code``、``source``；不以这些根开头的路径按 ``data.<path>``
解析，所以 ``title`` 与 ``data.title`` 等价。
"""

from __future__ import annotations

import json
import re
from typing import Any

from jinja2 import TemplateSyntaxError
from jinja2.exceptions import SecurityError
from jinja2.sandbox import SandboxedEnvironment

from src.collectors.base import CollectResult
from src.core.dag import NodeSpec, PortSpec
from src.core.exceptions import ValidationError
from src.core.task import Task
from src.processors.base import ProcessOutput

LOGIC_NODE_TYPE = "logic"
LOGIC_CONDITION = "condition"
LOGIC_FILTER = "filter"
LOGIC_TRANSFORM = "transform"
LOGIC_COMPONENTS = (LOGIC_CONDITION, LOGIC_FILTER, LOGIC_TRANSFORM)

RECORDS_PORT = "records"
ELSE_PORT = "else"
REJECTED_PORT = "rejected"
# condition 的分支 id 直接作为出端口名，这些名字留给固定端口
_RESERVED_CASE_IDS = frozenset({ELSE_PORT, "error", RECORDS_PORT, REJECTED_PORT})

OPERATORS = (
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "contains",
    "not_contains",
    "starts_with",
    "ends_with",
    "in",
    "not_in",
    "empty",
    "not_empty",
)
UNARY_OPERATORS = frozenset({"empty", "not_empty"})
AGGREGATE_FIELDS = ("count", "total", "failed", "success_rate")
QUANTIFIERS = ("any", "all")
LOGICAL_OPERATORS = ("and", "or")
TRANSFORM_TYPES = ("string", "number", "boolean", "json")
SORT_ORDERS = ("asc", "desc")

MAX_FILTER_LIMIT = 1_000_000
MAX_RENDERED_LENGTH = 100_000

_CASE_ID_RE = re.compile(r"^[A-Za-z0-9_]{1,40}$")
_FIELD_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
_PATH_ROOTS = frozenset({"data", "target", "metadata", "success", "error", "error_code", "source"})


# ── 记录视图与字段取值 ──────────────────────────────────────────────


def record_view(item: Any) -> dict[str, Any]:
    """把端口上的一条记录规整成统一视图，条件与模板都基于它取值。"""
    if isinstance(item, CollectResult):
        return {
            "data": item.data,
            "success": bool(item.success),
            "error": item.error,
            "error_code": item.error_code,
            "target": {
                "name": item.target.name,
                "type": item.target.target_type,
                "params": dict(item.target.params or {}),
            },
            "metadata": dict(item.metadata or {}),
            "source": item.target.name,
        }
    if isinstance(item, ProcessOutput):
        return {
            "data": item.data,
            "success": bool(item.success),
            "error": item.error,
            "error_code": None,
            "target": {},
            "metadata": dict(item.metadata or {}),
            "source": item.processor_name,
        }
    if isinstance(item, dict):
        # error 端口产出的错误记录，或其它裸 dict 记录
        return {
            "data": item,
            "success": bool(item.get("success", True)),
            "error": item.get("error"),
            "error_code": item.get("error_code"),
            "target": {},
            "metadata": {},
            "source": str(item.get("node_id") or ""),
        }
    return {
        "data": item,
        "success": True,
        "error": None,
        "error_code": None,
        "target": {},
        "metadata": {},
        "source": "",
    }


def is_usable(view: dict[str, Any]) -> bool:
    return bool(view.get("success")) and view.get("data") is not None


def resolve_path(view: dict[str, Any], path: str) -> Any:
    parts = [part for part in str(path or "").strip().split(".") if part]
    if not parts:
        return None
    if parts[0] not in _PATH_ROOTS:
        parts = ["data", *parts]
    current: Any = view
    for part in parts:
        if isinstance(current, dict):
            if part not in current:
                return None
            current = current[part]
        elif isinstance(current, (list, tuple)) and part.isdigit():
            index = int(part)
            if index >= len(current):
                return None
            current = current[index]
        else:
            return None
    return current


# ── 比较运算 ────────────────────────────────────────────────────────


def _to_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("true", "1", "yes", "y", "是"):
            return True
        if text in ("false", "0", "no", "n", "否", ""):
            return False
    return None


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _equals(actual: Any, expected: Any) -> bool:
    if actual is None or expected is None:
        return _is_empty(actual) and _is_empty(expected)
    if isinstance(actual, bool) or isinstance(expected, bool):
        left, right = _to_bool(actual), _to_bool(expected)
        return left is not None and left == right
    # 只有一侧真是数字时才按数值比较："007" 与 "7" 两个字符串仍按文本比较
    if isinstance(actual, (int, float)) or isinstance(expected, (int, float)):
        left_num, right_num = _to_number(actual), _to_number(expected)
        if left_num is not None and right_num is not None:
            return left_num == right_num
    return str(actual) == str(expected)


def _ordered(actual: Any, expected: Any, operator: str) -> bool:
    if actual is None or expected is None:
        return False
    left_num, right_num = _to_number(actual), _to_number(expected)
    left: Any
    right: Any
    if left_num is not None and right_num is not None:
        left, right = left_num, right_num
    else:
        # 非数值按文本比较，ISO 日期字符串因此也能直接比大小
        left, right = str(actual), str(expected)
    if operator == "gt":
        return left > right
    if operator == "gte":
        return left >= right
    if operator == "lt":
        return left < right
    return left <= right


def _as_list(expected: Any) -> list[Any]:
    if isinstance(expected, list):
        return expected
    if isinstance(expected, str):
        return [part.strip() for part in expected.split(",") if part.strip()]
    if expected is None:
        return []
    return [expected]


def _contains(actual: Any, expected: Any) -> bool:
    if actual is None:
        return False
    if isinstance(actual, str):
        return str(expected) in actual
    if isinstance(actual, (list, tuple, set)):
        return any(_equals(item, expected) for item in actual)
    if isinstance(actual, dict):
        return str(expected) in actual
    return str(expected) in str(actual)


def compare(actual: Any, operator: str, expected: Any = None) -> bool:
    if operator == "empty":
        return _is_empty(actual)
    if operator == "not_empty":
        return not _is_empty(actual)
    if operator == "eq":
        return _equals(actual, expected)
    if operator == "ne":
        return not _equals(actual, expected)
    if operator in ("gt", "gte", "lt", "lte"):
        return _ordered(actual, expected, operator)
    if operator == "contains":
        return _contains(actual, expected)
    if operator == "not_contains":
        return not _contains(actual, expected)
    if operator == "starts_with":
        return actual is not None and str(actual).startswith(str(expected))
    if operator == "ends_with":
        return actual is not None and str(actual).endswith(str(expected))
    if operator == "in":
        return any(_equals(actual, item) for item in _as_list(expected))
    if operator == "not_in":
        return not any(_equals(actual, item) for item in _as_list(expected))
    raise ValueError(f"unknown operator: {operator}")


def aggregate_stats(views: list[dict[str, Any]]) -> dict[str, float]:
    total = len(views)
    usable = sum(1 for view in views if is_usable(view))
    return {
        "count": usable,
        "total": total,
        "failed": total - usable,
        "success_rate": usable / total if total else 0.0,
    }


def _record_condition_matches(view: dict[str, Any], condition: dict[str, Any]) -> bool:
    return compare(
        resolve_path(view, condition.get("field", "")),
        condition.get("operator", "eq"),
        condition.get("value"),
    )


def _batch_condition_matches(
    condition: dict[str, Any],
    views: list[dict[str, Any]],
    stats: dict[str, float],
) -> bool:
    """condition 节点的单个条件：对整批记录求值。"""
    if condition.get("source") == "aggregate":
        return compare(stats.get(condition.get("field", "")), condition["operator"], condition.get("value"))
    usable = [view for view in views if is_usable(view)]
    if not usable:
        # 没有可用记录时 any/all 都不成立；判"有没有数据"请用聚合字段 count
        return False
    results = (_record_condition_matches(view, condition) for view in usable)
    if condition.get("quantifier", "any") == "all":
        return all(results)
    return any(results)


def _combine(results: list[bool], logical_operator: str) -> bool:
    if logical_operator == "or":
        return any(results)
    return all(results)


# ── 配置校验 ────────────────────────────────────────────────────────


def _validate_conditions(
    conditions: Any,
    *,
    where: str,
    batch: bool,
) -> list[str]:
    if not isinstance(conditions, list):
        return [f"{where}: conditions must be a list"]
    issues: list[str] = []
    for index, condition in enumerate(conditions):
        label = f"{where}.conditions[{index}]"
        if not isinstance(condition, dict):
            issues.append(f"{label} must be an object")
            continue
        source = condition.get("source", "record")
        if source not in ("record", "aggregate") or (source == "aggregate" and not batch):
            issues.append(f"{label}: invalid source '{source}'")
            continue
        field_path = condition.get("field")
        if source == "aggregate":
            if field_path not in AGGREGATE_FIELDS:
                issues.append(
                    f"{label}: aggregate field must be one of {', '.join(AGGREGATE_FIELDS)}"
                )
        elif not isinstance(field_path, str) or not field_path.strip():
            issues.append(f"{label}: field is required")
        operator = condition.get("operator")
        if operator not in OPERATORS:
            issues.append(f"{label}: unknown operator '{operator}'")
        elif operator not in UNARY_OPERATORS and "value" not in condition:
            issues.append(f"{label}: value is required for operator '{operator}'")
        if batch and source == "record":
            quantifier = condition.get("quantifier", "any")
            if quantifier not in QUANTIFIERS:
                issues.append(f"{label}: quantifier must be any or all")
    return issues


def _validate_condition_config(config: dict[str, Any]) -> list[str]:
    cases = config.get("cases")
    if not isinstance(cases, list) or not cases:
        return ["condition node needs at least one case"]
    issues: list[str] = []
    seen: set[str] = set()
    for index, case in enumerate(cases):
        where = f"cases[{index}]"
        if not isinstance(case, dict):
            issues.append(f"{where} must be an object")
            continue
        case_id = case.get("id")
        if (
            not isinstance(case_id, str)
            or not _CASE_ID_RE.match(case_id)
            or case_id in _RESERVED_CASE_IDS
        ):
            issues.append(
                f"{where}: case id must match [A-Za-z0-9_]{{1,40}} and not be "
                + ", ".join(sorted(_RESERVED_CASE_IDS))
            )
        elif case_id in seen:
            issues.append(f"{where}: duplicate case id '{case_id}'")
        else:
            seen.add(case_id)
        if case.get("logical_operator", "and") not in LOGICAL_OPERATORS:
            issues.append(f"{where}: logical_operator must be and or or")
        conditions = case.get("conditions")
        if not isinstance(conditions, list) or not conditions:
            issues.append(f"{where}: needs at least one condition")
            continue
        issues.extend(_validate_conditions(conditions, where=where, batch=True))
    return issues


def _validate_filter_config(config: dict[str, Any]) -> list[str]:
    issues = _validate_conditions(config.get("conditions", []), where="filter", batch=False)
    if config.get("logical_operator", "and") not in LOGICAL_OPERATORS:
        issues.append("filter: logical_operator must be and or or")
    if not isinstance(config.get("only_success", True), bool):
        issues.append("filter: only_success must be a boolean")
    dedupe_by = config.get("dedupe_by", [])
    if not isinstance(dedupe_by, list) or any(
        not isinstance(item, str) or not item.strip() for item in dedupe_by
    ):
        issues.append("filter: dedupe_by must be a list of field paths")
    sort_by = config.get("sort_by", "")
    if not isinstance(sort_by, str):
        issues.append("filter: sort_by must be a field path")
    if config.get("sort_order", "asc") not in SORT_ORDERS:
        issues.append("filter: sort_order must be asc or desc")
    limit = config.get("limit", 0)
    if not isinstance(limit, int) or isinstance(limit, bool) or not 0 <= limit <= MAX_FILTER_LIMIT:
        issues.append(f"filter: limit must be an integer between 0 and {MAX_FILTER_LIMIT}")
    return issues


def _validate_transform_config(config: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    fields = config.get("fields", [])
    drop_fields = config.get("drop_fields", [])
    if not isinstance(fields, list):
        return ["transform: fields must be a list"]
    if not isinstance(drop_fields, list) or any(
        not isinstance(item, str) or not item.strip() for item in drop_fields
    ):
        issues.append("transform: drop_fields must be a list of field names")
        drop_fields = []
    if not fields and not drop_fields:
        issues.append("transform: define at least one output field or drop field")
    seen: set[str] = set()
    for index, item in enumerate(fields):
        label = f"transform.fields[{index}]"
        if not isinstance(item, dict):
            issues.append(f"{label} must be an object")
            continue
        key = item.get("key")
        if not isinstance(key, str) or not _FIELD_KEY_RE.match(key):
            issues.append(f"{label}: key must be an identifier like title_upper")
        elif key in seen:
            issues.append(f"{label}: duplicate key '{key}'")
        else:
            seen.add(key)
        template = item.get("template")
        if not isinstance(template, str):
            issues.append(f"{label}: template must be a string")
        else:
            try:
                _TEMPLATE_ENV.parse(template)
            except TemplateSyntaxError as exc:
                issues.append(f"{label}: template syntax error: {exc.message}")
        if item.get("type", "string") not in TRANSFORM_TYPES:
            issues.append(f"{label}: type must be one of {', '.join(TRANSFORM_TYPES)}")
    if not isinstance(config.get("keep_original", True), bool):
        issues.append("transform: keep_original must be a boolean")
    return issues


def validate_logic_config(component: str, config: Any) -> list[str]:
    """逻辑节点配置校验；保存与执行前都会调用。"""
    if component not in LOGIC_COMPONENTS:
        return [f"unknown logic component '{component}'"]
    if config is None:
        config = {}
    if not isinstance(config, dict):
        return ["config must be an object"]
    if component == LOGIC_CONDITION:
        return _validate_condition_config(config)
    if component == LOGIC_FILTER:
        return _validate_filter_config(config)
    return _validate_transform_config(config)


def logic_output_port_names(component: str, config: Any) -> list[str]:
    if component == LOGIC_CONDITION:
        cases = config.get("cases") if isinstance(config, dict) else None
        names = [
            case["id"]
            for case in (cases if isinstance(cases, list) else [])
            if isinstance(case, dict) and isinstance(case.get("id"), str)
        ]
        return [*dict.fromkeys(names), ELSE_PORT]
    if component == LOGIC_FILTER:
        return [RECORDS_PORT, REJECTED_PORT]
    return [RECORDS_PORT]


def logic_node_ports(component: str, config: Any) -> tuple[list[PortSpec], list[PortSpec]]:
    """端口由配置推导（condition 的每个分支一个出端口），不信任客户端上报。"""
    ports_in = [PortSpec(RECORDS_PORT, required=True, type_hint="records")]
    ports_out = [
        PortSpec(name, required=True, type_hint="records")
        for name in logic_output_port_names(component, config)
    ]
    return ports_in, ports_out


# ── 模板沙箱 ────────────────────────────────────────────────────────


class _TemplateSandbox(SandboxedEnvironment):
    """Jinja2 沙箱：额外禁止幂运算与序列重复，避免模板制造超大对象卡死事件循环。"""

    intercepted_binops = frozenset({"*", "**"})

    def call_binop(self, context: Any, operator: str, left: Any, right: Any) -> Any:
        if operator == "**":
            raise SecurityError("'**' is not allowed in transform templates")
        if operator == "*" and (
            isinstance(left, (str, list, tuple)) or isinstance(right, (str, list, tuple))
        ):
            raise SecurityError("sequence repetition is not allowed in transform templates")
        return super().call_binop(context, operator, left, right)


_TEMPLATE_ENV = _TemplateSandbox(autoescape=False)


def _cast_value(text: str, value_type: str) -> Any:
    if value_type == "string":
        return text
    stripped = text.strip()
    if value_type == "number":
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            try:
                return float(stripped)
            except ValueError:
                return None
    if value_type == "boolean":
        return _to_bool(stripped)
    try:
        return json.loads(stripped) if stripped else None
    except json.JSONDecodeError:
        return None


def _with_data(item: Any, data: Any) -> Any:
    if isinstance(item, (CollectResult, ProcessOutput)):
        return item.model_copy(update={"data": data})
    return data


# ── 运行时节点 ──────────────────────────────────────────────────────


def _flatten(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        flat: list[Any] = []
        for item in value:
            if isinstance(item, list):
                flat.extend(item)
            else:
                flat.append(item)
        return flat
    return [value]


class LogicNode:
    """逻辑节点公共部分：配置在 setup 时复核，运行结果可声明激活的出端口。"""

    def __init__(
        self,
        spec: NodeSpec,
        *,
        task: Task,
        recovery_checkpoint: dict[str, Any],
    ) -> None:
        self.spec = spec
        self.node_id = spec.id
        self.input_ports = spec.ports_in
        self.output_ports = spec.ports_out
        self._task = task
        self.config: dict[str, Any] = dict(spec.config or {})
        # None 表示全部出端口都激活；condition 只激活命中的那一个分支
        self.active_ports: set[str] | None = None
        self.selected_branch: str | None = None

    async def setup(self) -> None:
        issues = validate_logic_config(self.spec.component, self.config)
        if issues:
            raise ValidationError(f"{self.spec.component} node {self.node_id}: {'; '.join(issues)}")

    async def teardown(self) -> None:
        return None


class ConditionNode(LogicNode):
    """IF / ELIF / ELSE：按顺序找第一个成立的分支，整批记录走这一支，其余分支跳过。"""

    async def run(self, ctx: Any) -> dict[str, Any]:
        records = _flatten(ctx.inputs.get(RECORDS_PORT))
        views = [record_view(item) for item in records]
        stats = aggregate_stats(views)
        cases = self.config.get("cases", [])
        selected = ELSE_PORT
        for case in cases:
            results = [
                _batch_condition_matches(condition, views, stats)
                for condition in case.get("conditions", [])
            ]
            if results and _combine(results, case.get("logical_operator", "and")):
                selected = case["id"]
                break
        outputs: dict[str, Any] = {case["id"]: [] for case in cases}
        outputs[ELSE_PORT] = []
        outputs[selected] = records
        self.active_ports = {selected}
        self.selected_branch = selected
        return outputs


def _sort_key(value: Any) -> tuple[int, Any]:
    number = _to_number(value)
    if number is not None:
        return (0, number)
    return (1, str(value))


class FilterNode(LogicNode):
    """按条件保留记录，并可去重、排序、截断；不满足条件的记录走 rejected 端口。"""

    async def run(self, ctx: Any) -> dict[str, Any]:
        records = _flatten(ctx.inputs.get(RECORDS_PORT))
        conditions = self.config.get("conditions", []) or []
        logical_operator = self.config.get("logical_operator", "and")
        only_success = self.config.get("only_success", True)

        kept: list[tuple[Any, dict[str, Any]]] = []
        rejected: list[Any] = []
        for item in records:
            view = record_view(item)
            if only_success and not is_usable(view):
                rejected.append(item)
                continue
            if conditions and not _combine(
                [_record_condition_matches(view, condition) for condition in conditions],
                logical_operator,
            ):
                rejected.append(item)
                continue
            kept.append((item, view))

        dedupe_by = [path for path in self.config.get("dedupe_by", []) if path.strip()]
        if dedupe_by:
            seen: set[str] = set()
            unique: list[tuple[Any, dict[str, Any]]] = []
            for item, view in kept:
                key = json.dumps(
                    [resolve_path(view, path) for path in dedupe_by],
                    sort_keys=True,
                    default=str,
                    ensure_ascii=False,
                )
                if key in seen:
                    continue
                seen.add(key)
                unique.append((item, view))
            kept = unique

        sort_by = str(self.config.get("sort_by") or "").strip()
        if sort_by:
            present = [pair for pair in kept if resolve_path(pair[1], sort_by) is not None]
            missing = [pair for pair in kept if resolve_path(pair[1], sort_by) is None]
            present.sort(
                key=lambda pair: _sort_key(resolve_path(pair[1], sort_by)),
                reverse=self.config.get("sort_order", "asc") == "desc",
            )
            kept = present + missing  # 缺字段的记录无论升降序都排最后

        limit = int(self.config.get("limit", 0) or 0)
        if limit:
            kept = kept[:limit]
        return {RECORDS_PORT: [item for item, _ in kept], REJECTED_PORT: rejected}


class TransformNode(LogicNode):
    """用 Jinja2 沙箱模板逐条计算字段（类似 Dify Template / n8n Edit Fields）。"""

    async def setup(self) -> None:
        await super().setup()
        self._compiled = [
            (item["key"], _TEMPLATE_ENV.from_string(item["template"]), item.get("type", "string"))
            for item in self.config.get("fields", [])
        ]

    async def run(self, ctx: Any) -> dict[str, Any]:
        records = _flatten(ctx.inputs.get(RECORDS_PORT))
        keep_original = self.config.get("keep_original", True)
        drop_fields = [name for name in self.config.get("drop_fields", []) if name.strip()]
        outputs: list[Any] = []
        for index, item in enumerate(records):
            view = record_view(item)
            if not is_usable(view):
                outputs.append(item)  # 失败记录原样透传，不参与转换
                continue
            data = view["data"]
            if keep_original:
                base: dict[str, Any] = dict(data) if isinstance(data, dict) else {"value": data}
            else:
                base = {}
            for name in drop_fields:
                base.pop(name, None)
            context = {**view, "index": index}
            for key, template, value_type in self._compiled:
                rendered = template.render(context)
                if len(rendered) > MAX_RENDERED_LENGTH:
                    raise ValueError(
                        f"transform field '{key}' rendered more than {MAX_RENDERED_LENGTH} characters"
                    )
                base[key] = _cast_value(rendered, value_type)
            outputs.append(_with_data(item, base))
        return {RECORDS_PORT: outputs}


_LOGIC_NODE_CLASSES: dict[str, type[LogicNode]] = {
    LOGIC_CONDITION: ConditionNode,
    LOGIC_FILTER: FilterNode,
    LOGIC_TRANSFORM: TransformNode,
}


def instantiate_logic_node(
    spec: NodeSpec,
    *,
    task: Task,
    recovery_checkpoint: dict[str, Any],
) -> LogicNode:
    node_class = _LOGIC_NODE_CLASSES.get(spec.component)
    if node_class is None:
        raise ValidationError(f"unknown logic component: {spec.component}")
    return node_class(spec, task=task, recovery_checkpoint=recovery_checkpoint)


# ── 编辑器目录 ──────────────────────────────────────────────────────

DEFAULT_LOGIC_CONFIGS: dict[str, dict[str, Any]] = {
    LOGIC_CONDITION: {
        "cases": [
            {
                "id": "case_1",
                "label": "",
                "logical_operator": "and",
                "conditions": [
                    {"source": "aggregate", "field": "count", "operator": "gt", "value": 0}
                ],
            }
        ]
    },
    LOGIC_FILTER: {
        "logical_operator": "and",
        "conditions": [],
        "only_success": True,
        "dedupe_by": [],
        "sort_by": "",
        "sort_order": "asc",
        "limit": 0,
    },
    LOGIC_TRANSFORM: {
        "fields": [{"key": "summary", "template": "{{ data.title }}", "type": "string"}],
        "keep_original": True,
        "drop_fields": [],
    },
}

_LOGIC_COPY = {
    LOGIC_CONDITION: (
        "Condition",
        "Routes the whole record batch to the first matching case (IF / ELIF / ELSE); "
        "the other branches are skipped.",
    ),
    LOGIC_FILTER: (
        "Filter",
        "Keeps records that match the conditions, then optionally dedupes, sorts and "
        "limits them; non-matching records go to the rejected port.",
    ),
    LOGIC_TRANSFORM: (
        "Transform",
        "Computes record fields with sandboxed Jinja2 templates, e.g. "
        "{{ data.title | upper }}.",
    ),
}


def logic_node_catalog() -> list[dict[str, Any]]:
    """核心内置逻辑节点的编辑器定义（不来自插件，owner 固定为 core）。"""
    payload: list[dict[str, Any]] = []
    for component in LOGIC_COMPONENTS:
        display_name, description = _LOGIC_COPY[component]
        default_config = json.loads(json.dumps(DEFAULT_LOGIC_CONFIGS[component]))
        ports_in, ports_out = logic_node_ports(component, default_config)
        payload.append(
            {
                "type": LOGIC_NODE_TYPE,
                "component": component,
                "display_name": display_name,
                "description": description,
                "ports_in": [
                    {"name": p.name, "required": p.required, "type_hint": p.type_hint}
                    for p in ports_in
                ],
                "ports_out": [
                    {"name": p.name, "required": p.required, "type_hint": p.type_hint}
                    for p in ports_out
                ],
                "config_schema": {},
                "output_fields": [],
                "default_config": default_config,
                "operators": list(OPERATORS),
                "aggregate_fields": list(AGGREGATE_FIELDS),
                "owner": "core",
            }
        )
    return payload
