from sqlalchemy import Float, BigInteger, String, Integer, Boolean, UUID
from sqlalchemy.orm import declarative_base, mapped_column, Mapped

Base = declarative_base()


class ChangefeedJobDetails(Base):
    __tablename__ = "changefeed_job_details"

    workflow_id: Mapped[str] = mapped_column(UUID(as_uuid=True), primary_key=True)
    job_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    description: Mapped[str] = mapped_column(String)
    error: Mapped[str] = mapped_column(String)
    high_water_timestamp: Mapped[float] = mapped_column(Float)
    is_initial_scan_only: Mapped[bool] = mapped_column(Boolean, nullable=True)
    finished_ago_seconds: Mapped[int] = mapped_column(Integer, nullable=True)
    latency: Mapped[int] = mapped_column(Integer, nullable=True)
    running_status: Mapped[str] = mapped_column(String, nullable=True)
    status: Mapped[str] = mapped_column(String)

