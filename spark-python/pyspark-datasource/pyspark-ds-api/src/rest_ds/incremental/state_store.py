"""Database-backed state store for incremental REST API ingestion.

This is the control-plane for incremental runs: it is intentionally a plain
SQLAlchemy/SQLModel connection, independent of the Spark session used to
process the fetched records. Control-table reads/writes are small,
transactional, single-row operations — running them through Spark would add
distributed-execution overhead for no benefit. This mirrors how most
production ingestion frameworks (Airbyte, Fivetran, dbt sources, Meltano)
separate "state" (a fast OLTP-style store) from "data" (the distributed
processing engine).

Any SQLAlchemy-compatible URL works, so the same code path supports:

- ``sqlite:///incremental_state.db``            (local development / tests)
- ``postgresql+psycopg2://user:pass@host/db``   (shared control table in prod)
- ``mysql+pymysql://user:pass@host/db``
"""

from datetime import datetime, timezone

from sqlmodel import Session, SQLModel, create_engine, select

from rest_ds.incremental.models import IngestionRunHistory, IngestionWatermark


class IncrementalStateStore:
    """CRUD wrapper around the ``ingestion_watermark`` / ``ingestion_run_history`` tables."""

    def __init__(self, db_url: str, echo: bool = False):
        self.db_url = db_url
        self.engine = create_engine(db_url, echo=echo)
        SQLModel.metadata.create_all(self.engine)

    # ------------------------------------------------------------------ #
    # Watermark reads
    # ------------------------------------------------------------------ #

    def get_watermark(self, source_name: str, default: str) -> str:
        """Return the last successful watermark for ``source_name``, or
        ``default`` (typically the YAML ``incremental.initialValue``) if this
        is the first run."""
        with Session(self.engine) as session:
            row = session.get(IngestionWatermark, source_name)
            return row.watermark_value if row else default

    def get_history(
        self, source_name: str, limit: int = 20
    ) -> list[IngestionRunHistory]:
        """Return the most recent run-history rows for a source, newest first.
        Useful for reconciliation and troubleshooting."""
        with Session(self.engine) as session:
            statement = (
                select(IngestionRunHistory)
                .where(IngestionRunHistory.source_name == source_name)
                .order_by(IngestionRunHistory.started_at.desc())  # type: ignore[attr-defined]
                .limit(limit)
            )
            return list(session.exec(statement))

    # ------------------------------------------------------------------ #
    # Run lifecycle
    # ------------------------------------------------------------------ #

    def start_run(
        self, source_name: str, watermark_start: str, params_used: str
    ) -> int:
        """Record the start of a run and return its ``run_id``."""
        with Session(self.engine) as session:
            run = IngestionRunHistory(
                source_name=source_name,
                status="running",
                watermark_start=watermark_start,
                params_used=params_used,
            )
            session.add(run)
            session.commit()
            session.refresh(run)
            assert run.run_id is not None  # set by the DB on commit
            return run.run_id

    def complete_run(
        self,
        run_id: int,
        source_name: str,
        watermark_end: str,
        records_fetched: int,
    ) -> None:
        """Mark a run as successful and advance the source's watermark.

        The watermark is only ever advanced here — on failure it is left
        untouched so the next run retries the same window."""
        now = datetime.now(timezone.utc)
        with Session(self.engine) as session:
            run = session.get(IngestionRunHistory, run_id)
            if run is None:
                raise ValueError(f"No run_history row found for run_id={run_id}")
            run.status = "success"
            run.watermark_end = watermark_end
            run.records_fetched = records_fetched
            run.completed_at = now
            session.add(run)

            watermark_row = session.get(IngestionWatermark, source_name)
            if watermark_row:
                watermark_row.watermark_value = watermark_end
                watermark_row.updated_at = now
            else:
                watermark_row = IngestionWatermark(
                    source_name=source_name,
                    watermark_value=watermark_end,
                    updated_at=now,
                )
            session.add(watermark_row)
            session.commit()

    def fail_run(self, run_id: int, error_message: str) -> None:
        """Mark a run as failed. The watermark is left untouched."""
        with Session(self.engine) as session:
            run = session.get(IngestionRunHistory, run_id)
            if run is None:
                raise ValueError(f"No run_history row found for run_id={run_id}")
            run.status = "failed"
            run.error_message = error_message[:2000]
            run.completed_at = datetime.now(timezone.utc)
            session.add(run)
            session.commit()
