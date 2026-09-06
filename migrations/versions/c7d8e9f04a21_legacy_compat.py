"""legacy compat: bring pre-Alembic create_all databases to baseline shape.

遗留库（历史上由 create_all + 手写 ALTER 建成）可能缺少后来追加的列。
收编流程现在 stamp 到 **baseline**（而非 head），因此本 revision 会在
"upgrade head" 时对遗留库执行：幂等补齐缺失列/索引；对全新库则是 no-op
（baseline 已包含全部列）。

Revision ID: c7d8e9f04a21
Revises: 86215de297eb
Create Date: 2026-09-05 15:10:00.000000

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

import src.storage.models

# revision identifiers, used by Alembic.
revision: str = "c7d8e9f04a21"
down_revision: Union[str, None] = "86215de297eb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _columns(conn, table: str) -> set[str]:
    inspector = sa.inspect(conn)
    return {col["name"] for col in inspector.get_columns(table)}


def upgrade() -> None:
    conn = op.get_bind()
    cols = _columns(conn, "scheduler_states")
    if "metadata" not in cols:
        op.add_column(
            "scheduler_states",
            sa.Column("metadata", src.storage.models.JSONType(), nullable=True),
        )
    if "stored_at" not in cols:
        op.add_column("scheduler_states", sa.Column("stored_at", sa.DateTime(), nullable=True))
    if "task_status" not in cols:
        op.add_column(
            "scheduler_states", sa.Column("task_status", sa.String(), nullable=True)
        )
        indexes = {idx["name"] for idx in sa.inspect(conn).get_indexes("scheduler_states")}
        if "ix_scheduler_states_task_status" not in indexes:
            op.create_index(
                "ix_scheduler_states_task_status", "scheduler_states", ["task_status"]
            )


def downgrade() -> None:
    # 遗留兼容列不做 downgrade（删除列有数据丢失风险；回滚到更旧版本
    # 的场景应走整库重建）
    pass
