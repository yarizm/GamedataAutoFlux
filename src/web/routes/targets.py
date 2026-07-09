"""Target import API — TXT 文件批量导入采集目标。"""

from __future__ import annotations

from fastapi import APIRouter, File, Form, UploadFile, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(tags=["targets"])


class ImportTargetsResponse(BaseModel):
    """TXT 导入响应。"""

    targets: list[dict] = Field(default_factory=list)
    total: int = 0
    skipped: int = 0
    skipped_reasons: list[str] = Field(default_factory=list)


@router.post("/tasks/import-targets", response_model=ImportTargetsResponse)
async def import_targets(
    file: UploadFile = File(...),
    collector_name: str = Form(...),
    target_type: str = Form(...),
):
    """从 TXT 文件批量导入采集目标。

    解析规则:
    - 跳过 # 开头注释行和空行
    - 每行取第一个 token（空格/Tab 分隔）
    - 无效行记入 skipped，不阻断整体
    """
    if not collector_name.startswith("youtube_"):
        raise HTTPException(
            400, f"不支持的 collector: {collector_name}")

    if target_type not in ("youtube_channel", "youtube_video"):
        raise HTTPException(
            400, f"不支持的 target_type: {target_type}")

    content = await file.read()
    text = content.decode("utf-8-sig")

    targets: list[dict] = []
    skipped = 0
    skipped_reasons: list[str] = []

    for line_no, raw_line in enumerate(text.splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        url = line.split()[0] if line else ""

        # 基本 URL 校验
        if not url:
            skipped += 1
            skipped_reasons.append(f"行 {line_no}: 空内容")
            continue

        # 清理 URL
        if url.startswith("//"):
            url = "https:" + url
        elif url.startswith("/"):
            url = "https://www.youtube.com" + url
        elif not url.startswith("http"):
            url = "https://" + url

        params: dict = {}
        if target_type == "youtube_channel":
            params["channel_url"] = url
        elif target_type == "youtube_video":
            params["video_url"] = url

        targets.append({
            "name": url,
            "target_type": target_type,
            "params": params,
        })

    return ImportTargetsResponse(
        targets=targets,
        total=len(targets),
        skipped=skipped,
        skipped_reasons=skipped_reasons[-50:],  # 上限防过大
    )
