"""
Excel 报告导出器。

使用 openpyxl 将结构化数据写入多 Sheet 的 .xlsx 文件，
支持样式美化和图表嵌入。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger
from openpyxl import Workbook
from openpyxl.chart import BarChart, LineChart, Reference
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from src.reporting.data_extractor import ExtractedData


# ==================== 样式常量 ====================

HEADER_FONT = Font(name="微软雅黑", bold=True, size=11, color="FFFFFF")
HEADER_FILL = PatternFill(start_color="2F5496", end_color="2F5496", fill_type="solid")
HEADER_ALIGNMENT = Alignment(horizontal="center", vertical="center", wrap_text=True)
HEADER_BORDER = Border(
    bottom=Side(style="thin", color="1F3864"),
    right=Side(style="thin", color="D9E2F3"),
)

DATA_FONT = Font(name="微软雅黑", size=10)
DATA_ALIGNMENT = Alignment(vertical="top", wrap_text=True)
DATA_BORDER = Border(
    bottom=Side(style="hair", color="D9E2F3"),
    right=Side(style="hair", color="D9E2F3"),
)

ALT_ROW_FILL = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")


def export_to_excel(
    data: ExtractedData,
    output_path: str | Path,
    title: str = "GamedataAutoFlux 数据报告",
    llm_content: str | None = None,
) -> Path:
    """
    将提取后的数据导出为 Excel 文件。

    Args:
        data: DataExtractor 提取的结构化数据
        output_path: 输出文件路径
        title: 报告标题（写入 Sheet 标题行）
        llm_content: LLM 生成的分析文本（可选）

    Returns:
        生成的 Excel 文件路径
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()

    # 删除默认 Sheet
    default_sheet = wb.active
    wb.remove(default_sheet)

    # Sheet 1: 游戏概览
    if data.overview:
        _write_overview_sheet(wb, data.overview, title)

    # Sheet 2: 评论明细
    if data.reviews:
        _write_reviews_sheet(wb, data.reviews)

    # Sheet 3: 趋势数据
    if data.trends:
        _write_trends_sheet(wb, data.trends)

    # Sheet 4: 相关搜索词
    if data.related_queries:
        _write_related_queries_sheet(wb, data.related_queries)

    # Sheet 5: LLM 分析
    if llm_content:
        _write_llm_sheet(wb, llm_content, title)

    # 如果没有任何数据，创建空的说明页
    if not wb.sheetnames:
        ws = wb.create_sheet("说明")
        ws["A1"] = "暂无可用数据。请先执行采集任务并落库后再生成报告。"
        ws["A1"].font = Font(name="微软雅黑", size=12, bold=True)

    wb.save(str(output_path))
    logger.info(f"[ExcelExporter] 报告已生成: {output_path} ({len(wb.sheetnames)} sheets)")
    return output_path


# ==================== Sheet 写入函数 ====================

def _write_overview_sheet(wb: Workbook, rows: list[dict[str, Any]], title: str) -> None:
    """写入游戏概览 Sheet。"""
    ws = wb.create_sheet("游戏概览")
    headers = _collect_headers(rows)

    # 写标题行
    _write_header_row(ws, headers, row_num=1)

    # 写数据行
    for i, row in enumerate(rows, start=2):
        for j, header in enumerate(headers, start=1):
            cell = ws.cell(row=i, column=j, value=row.get(header, ""))
            cell.font = DATA_FONT
            cell.alignment = DATA_ALIGNMENT
            cell.border = DATA_BORDER
            if i % 2 == 0:
                cell.fill = ALT_ROW_FILL

    _auto_column_width(ws, headers)
    ws.freeze_panes = "A2"

    # 如果有数值列，生成图表
    _add_overview_chart(ws, headers, rows)


def _write_reviews_sheet(wb: Workbook, rows: list[dict[str, Any]]) -> None:
    """写入评论明细 Sheet。"""
    ws = wb.create_sheet("评论明细")
    headers = _collect_headers(rows)

    _write_header_row(ws, headers, row_num=1)

    for i, row in enumerate(rows, start=2):
        for j, header in enumerate(headers, start=1):
            cell = ws.cell(row=i, column=j, value=row.get(header, ""))
            cell.font = DATA_FONT
            cell.alignment = DATA_ALIGNMENT
            cell.border = DATA_BORDER
            if i % 2 == 0:
                cell.fill = ALT_ROW_FILL

    _auto_column_width(ws, headers, max_width=60)
    ws.freeze_panes = "A2"


def _write_trends_sheet(wb: Workbook, rows: list[dict[str, Any]]) -> None:
    """写入趋势数据 Sheet 并生成折线图。"""
    ws = wb.create_sheet("趋势数据")
    headers = _collect_headers(rows)

    _write_header_row(ws, headers, row_num=1)

    for i, row in enumerate(rows, start=2):
        for j, header in enumerate(headers, start=1):
            cell = ws.cell(row=i, column=j, value=row.get(header, ""))
            cell.font = DATA_FONT
            cell.alignment = DATA_ALIGNMENT
            cell.border = DATA_BORDER
            if i % 2 == 0:
                cell.fill = ALT_ROW_FILL

    _auto_column_width(ws, headers)
    ws.freeze_panes = "A2"

    # 生成趋势折线图
    _add_trends_chart(ws, headers, rows)


def _write_related_queries_sheet(wb: Workbook, rows: list[dict[str, Any]]) -> None:
    """写入相关搜索词 Sheet。"""
    ws = wb.create_sheet("相关搜索词")
    headers = _collect_headers(rows)

    _write_header_row(ws, headers, row_num=1)

    for i, row in enumerate(rows, start=2):
        for j, header in enumerate(headers, start=1):
            cell = ws.cell(row=i, column=j, value=row.get(header, ""))
            cell.font = DATA_FONT
            cell.alignment = DATA_ALIGNMENT
            cell.border = DATA_BORDER
            if i % 2 == 0:
                cell.fill = ALT_ROW_FILL

    _auto_column_width(ws, headers)
    ws.freeze_panes = "A2"

    # 添加相关搜索词条形图
    _add_related_queries_chart(ws, headers, rows)


def _write_llm_sheet(wb: Workbook, content: str, title: str) -> None:
    """写入 LLM 分析报告 Sheet。"""
    ws = wb.create_sheet("AI 分析报告")

    # 标题
    ws["A1"] = title
    ws["A1"].font = Font(name="微软雅黑", bold=True, size=14, color="2F5496")
    ws.merge_cells("A1:H1")

    # 内容 —— 按段落分行写入
    paragraphs = content.split("\n")
    current_row = 3
    for para in paragraphs:
        para = para.strip()
        if not para:
            current_row += 1
            continue

        cell = ws.cell(row=current_row, column=1, value=para)

        # Markdown 标题检测
        if para.startswith("# "):
            cell.value = para.lstrip("# ")
            cell.font = Font(name="微软雅黑", bold=True, size=14, color="2F5496")
        elif para.startswith("## "):
            cell.value = para.lstrip("# ")
            cell.font = Font(name="微软雅黑", bold=True, size=12, color="2F5496")
        elif para.startswith("### "):
            cell.value = para.lstrip("# ")
            cell.font = Font(name="微软雅黑", bold=True, size=11)
        elif para.startswith("- ") or para.startswith("* "):
            cell.font = Font(name="微软雅黑", size=10)
            cell.alignment = Alignment(indent=2)
        else:
            cell.font = Font(name="微软雅黑", size=10)

        cell.alignment = Alignment(wrap_text=True, vertical="top")
        ws.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=8)
        current_row += 1

    ws.column_dimensions["A"].width = 120


# ==================== 图表生成 ====================

def _add_overview_chart(ws, headers: list[str], rows: list[dict[str, Any]]) -> None:
    """在概览 Sheet 中生成柱状图（当前在线/评论总量对比）。"""
    if len(rows) < 1:
        return

    # 查找数值列
    numeric_cols = []
    for h in ["当前在线", "评论总量"]:
        if h in headers:
            numeric_cols.append(h)

    if not numeric_cols:
        return

    # 构建辅助数据区域（在数据区域右侧）
    chart_start_col = len(headers) + 3
    chart_start_row = 1

    # 写图表标题行
    ws.cell(row=chart_start_row, column=chart_start_col, value="游戏名")
    for k, col_name in enumerate(numeric_cols, start=1):
        ws.cell(row=chart_start_row, column=chart_start_col + k, value=col_name)

    for i, row in enumerate(rows, start=1):
        ws.cell(row=chart_start_row + i, column=chart_start_col, value=row.get("游戏名", ""))
        for k, col_name in enumerate(numeric_cols, start=1):
            val = row.get(col_name, 0)
            ws.cell(row=chart_start_row + i, column=chart_start_col + k, value=val if isinstance(val, (int, float)) else 0)

    # 生成柱状图
    chart = BarChart()
    chart.type = "col"
    chart.title = "游戏核心指标对比"
    chart.y_axis.title = "数值"
    chart.style = 10
    chart.width = 20
    chart.height = 12

    data_ref = Reference(
        ws,
        min_col=chart_start_col + 1,
        max_col=chart_start_col + len(numeric_cols),
        min_row=chart_start_row,
        max_row=chart_start_row + len(rows),
    )
    cats_ref = Reference(
        ws,
        min_col=chart_start_col,
        min_row=chart_start_row + 1,
        max_row=chart_start_row + len(rows),
    )
    chart.add_data(data_ref, titles_from_data=True)
    chart.set_categories(cats_ref)

    ws.add_chart(chart, f"A{len(rows) + 4}")


def _add_trends_chart(ws, headers: list[str], rows: list[dict[str, Any]]) -> None:
    """在趋势 Sheet 中生成折线图。"""
    # 过滤出搜索热度类型的数据
    heat_rows = [r for r in rows if r.get("类型") == "搜索热度" and isinstance(r.get("热度值"), (int, float))]

    if len(heat_rows) < 2:
        return

    # 查找日期和热度值列索引
    if "日期" not in headers or "热度值" not in headers:
        return

    date_col = headers.index("日期") + 1
    value_col = headers.index("热度值") + 1

    chart = LineChart()
    chart.title = "搜索热度趋势"
    chart.y_axis.title = "热度 (0-100)"
    chart.x_axis.title = "日期"
    chart.style = 10
    chart.width = 28
    chart.height = 14

    # 找到第一个和最后一个搜索热度行
    first_heat_row = None
    last_heat_row = None
    for i, row in enumerate(rows, start=2):
        if row.get("类型") == "搜索热度":
            if first_heat_row is None:
                first_heat_row = i
            last_heat_row = i

    if first_heat_row is None or last_heat_row is None:
        return

    data_ref = Reference(ws, min_col=value_col, min_row=first_heat_row - 1, max_row=last_heat_row)
    cats_ref = Reference(ws, min_col=date_col, min_row=first_heat_row, max_row=last_heat_row)

    chart.add_data(data_ref, titles_from_data=True)
    chart.set_categories(cats_ref)

    ws.add_chart(chart, f"A{len(rows) + 4}")


def _add_related_queries_chart(ws, headers: list[str], rows: list[dict[str, Any]]) -> None:
    """在相关搜索词 Sheet 中生成水平条形图。"""
    # 只取 top 10 热门词
    top_rows = [r for r in rows if r.get("类型") == "热门"][:10]
    if len(top_rows) < 2:
        return

    # 在数据右侧构建辅助区域
    chart_start_col = len(headers) + 3
    chart_start_row = 1

    ws.cell(row=chart_start_row, column=chart_start_col, value="查询词")
    ws.cell(row=chart_start_row, column=chart_start_col + 1, value="热度值")

    for i, row in enumerate(top_rows, start=1):
        ws.cell(row=chart_start_row + i, column=chart_start_col, value=row.get("查询词", ""))
        val = row.get("热度值", 0)
        ws.cell(row=chart_start_row + i, column=chart_start_col + 1, value=val if isinstance(val, (int, float)) else 0)

    chart = BarChart()
    chart.type = "bar"
    chart.title = "热门相关搜索词 Top 10"
    chart.y_axis.title = "查询词"
    chart.x_axis.title = "热度值"
    chart.style = 10
    chart.width = 22
    chart.height = 14

    data_ref = Reference(ws, min_col=chart_start_col + 1, min_row=chart_start_row, max_row=chart_start_row + len(top_rows))
    cats_ref = Reference(ws, min_col=chart_start_col, min_row=chart_start_row + 1, max_row=chart_start_row + len(top_rows))

    chart.add_data(data_ref, titles_from_data=True)
    chart.set_categories(cats_ref)

    ws.add_chart(chart, f"A{len(rows) + 4}")


# ==================== 工具函数 ====================

def _collect_headers(rows: list[dict[str, Any]]) -> list[str]:
    """从多行数据中收集所有出现过的列名（保持插入顺序）。"""
    seen: set[str] = set()
    headers: list[str] = []
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                headers.append(key)
    return headers


def _write_header_row(ws, headers: list[str], row_num: int = 1) -> None:
    """写入带样式的标题行。"""
    for j, header in enumerate(headers, start=1):
        cell = ws.cell(row=row_num, column=j, value=header)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = HEADER_ALIGNMENT
        cell.border = HEADER_BORDER


def _auto_column_width(ws, headers: list[str], max_width: int = 40) -> None:
    """根据内容自动调整列宽。"""
    for j, header in enumerate(headers, start=1):
        col_letter = get_column_letter(j)
        # 取标题宽度和内容最大宽度中的较大值
        header_width = len(header) * 2.2  # CJK 字符加权
        content_width = 0
        for row in ws.iter_rows(min_row=2, min_col=j, max_col=j):
            for cell in row:
                if cell.value:
                    val_str = str(cell.value)
                    # CJK 字符宽度约等于 2 个英文字符
                    w = sum(2 if ord(c) > 127 else 1 for c in val_str[:50])
                    content_width = max(content_width, w)

        width = min(max(header_width, content_width + 2), max_width)
        ws.column_dimensions[col_letter].width = max(width, 8)
