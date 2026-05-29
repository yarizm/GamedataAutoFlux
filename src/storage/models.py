from datetime import datetime, timezone

from sqlalchemy import Column, String, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.types import JSON
from sqlalchemy.ext.compiler import compiles

try:
    from pgvector.sqlalchemy import Vector

    VectorType = Vector(1536)
except ImportError:
    VectorType = JSON

Base = declarative_base()


# Custom JSON type that falls back to JSONB on PostgreSQL for better performance
class JSONType(JSON):
    pass


@compiles(JSONType, "postgresql")
def compile_json_postgresql(type_, compiler, **kw):
    return "JSONB"


def utcnow():
    return datetime.now(timezone.utc)


class RecordModel(Base):
    __tablename__ = "records"

    key = Column(String, primary_key=True)
    source = Column(String, default="", index=True)
    collector = Column(String, default="", index=True)
    game_name = Column(String, default="", index=True)
    app_id = Column(String, default="", index=True)
    group_id = Column(String, default="")
    task_id = Column(String, default="")

    metadata_ = Column("metadata", JSONType, default=dict)
    tags = Column(JSONType, default=list)
    data = Column(JSONType, default=dict)
    embedding = Column(VectorType)

    stored_at = Column(DateTime, default=utcnow, index=True)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class SchedulerStateModel(Base):
    __tablename__ = "scheduler_states"

    key = Column(String, primary_key=True)
    state_type = Column(String, nullable=False, index=True)
    data = Column(JSONType, default=dict)
    metadata_ = Column("metadata", JSONType, default=dict)
    task_status = Column(String, nullable=True, index=True)
    stored_at = Column(DateTime, default=utcnow, index=True)
    updated_at = Column(DateTime, default=utcnow, onupdate=utcnow)


class AgentSessionModel(Base):
    __tablename__ = "agent_sessions"

    session_id = Column(String, primary_key=True)
    messages = Column(JSONType, default=list)
    last_active_at = Column(DateTime, default=utcnow)
