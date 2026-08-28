"""SQLModel table definitions for the incremental ingestion control tables.

Two tables back the incremental framework:

- ``ingestion_watermark`` — one row per source, holding the last successful
  high-watermark value. This is the "bookmark" read at the start of every run
  and advanced only after a run succeeds.
- ``ingestion_run_history`` — an append-only audit log of every run attempt
  (running / success / failed), including the parameters used, row counts,
  and error messages. This is the basis for reconciliation, duplicate
  detection, and troubleshooting (see `examples/incremental/README.md`).
"""

from datetime import datetime, timezone
from typing import Optional

from sqlmodel import Field, SQLModel


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class IngestionWatermark(SQLModel, table=True):
    """Current incremental pointer for a single data source."""

    __tablename__ = "ingestion_watermark"

    source_name: str = Field(primary_key=True)
    watermark_value: str
    updated_at: datetime = Field(default_factory=_utcnow)


class IngestionRunHistory(SQLModel, table=True):
    """Append-only audit trail of every ingestion run attempt."""

    __tablename__ = "ingestion_run_history"

    run_id: Optional[int] = Field(default=None, primary_key=True)
    source_name: str = Field(index=True)
    status: str = Field(default="running")  # running | success | failed
    watermark_start: Optional[str] = None
    watermark_end: Optional[str] = None
    params_used: Optional[str] = None
    records_fetched: Optional[int] = None
    error_message: Optional[str] = None
    started_at: datetime = Field(default_factory=_utcnow)
    completed_at: Optional[datetime] = None
