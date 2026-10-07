"""backfill ix_scheduler_states_stored_at on legacy databases.

baseline 已为全新库创建该索引；历史 create_all 库被 stamp 到 baseline，
但从未真正执行过 baseline 的 DDL，因此缺少它。models 声明
``SchedulerStateModel.stored_at`` 为 ``index=True``，这里把真库补齐。

Revision ID: d4f1a7c2b903
Revises: c7d8e9f04a21
Create Date: 2026-10-07 11:45:00.000000

"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d4f1a7c2b903"
down_revision: Union[str, None] = "c7d8e9f04a21"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    indexes = {idx["name"] for idx in sa.inspect(conn).get_indexes("scheduler_states")}
    if "ix_scheduler_states_stored_at" not in indexes:
        op.create_index("ix_scheduler_states_stored_at", "scheduler_states", ["stored_at"])


def downgrade() -> None:
    # 与 legacy_compat 一致：不做 downgrade（回滚到更旧版本应走整库重建）
    pass
