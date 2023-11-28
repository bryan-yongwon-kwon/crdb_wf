from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy import Column, String, DateTime, Enum
from sqlalchemy.orm import declarative_base, mapped_column, Mapped
from sqlalchemy.sql import func
import enum
import uuid
import datetime

Base = declarative_base()


class WorkflowStatus(enum.Enum):
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"


class CRDBDbOpsWFEntry(Base):
    """The CRDBDbOpsWFEntry class corresponds to the "crdb_dbops_wf_entry" database table."""
    __tablename__ = "crdb_dbops_wf_entry"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[str] = mapped_column(String)  # New column to store APScheduler job_id
    cluster_name: Mapped[str] = mapped_column(String)
    region: Mapped[str] = mapped_column(String)
    deployment_env: Mapped[str] = mapped_column(String)
    operation_type: Mapped[str] = mapped_column(String)
    operator_name: Mapped[str] = mapped_column(String)
    status: Mapped[WorkflowStatus] = mapped_column(Enum(WorkflowStatus))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self) -> str:
        return f"CRDBDbOpsWFEntry(id={self.id!r}, job_id={self.job_id!r}, cluster_name={self.cluster_name!r}, region={self.region!r}, \
                deployment_env={self.deployment_env!r}, operation_type={self.operation_type!r}, \
                operator_name={self.operator_name!r}, status={self.status!r}, \
                created_at={self.created_at!r}, updated_at={self.updated_at!r})"


class CRDBDbOpsWFEvent(Base):
    """The CRDBDbOpsWFEvent class corresponds to the "crdb_dbops_wf_event" database table."""
    __tablename__ = "crdb_dbops_wf_event"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workflow_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    cluster_name: Mapped[str] = mapped_column(String)
    region: Mapped[str] = mapped_column(String)
    deployment_env: Mapped[str] = mapped_column(String)
    operation_type: Mapped[str] = mapped_column(String)
    operator_name: Mapped[str] = mapped_column(String)
    event_detail: Mapped[str] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self) -> str:
        return f"CRDBDbOpsWFEvent(id={self.id!r}, workflow_id={self.workflow_id!r}, cluster_name={self.cluster_name!r}, \
                region={self.region!r}, deployment_env={self.deployment_env!r}, operation_type={self.operation_type!r}, \
                operator_name={self.operator_name!r}, event_detail={self.event_detail!r}, \
                created_at={self.created_at!r})"
