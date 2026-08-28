"""Mock REST API for exercising incremental ingestion locally.

Serves `/events`, a flat JSON list of records each with an `id` and an
`updated_at` timestamp. Supports an `updated_since` query parameter
(ISO-8601, exclusive lower bound), matching the parameter the incremental
runner injects on every run.

Run with:

    PYTHONPATH=src uv run python examples/incremental/mock_incremental_server.py
"""

from datetime import datetime, timedelta, timezone
from typing import List, Optional

import uvicorn
from fastapi import FastAPI, Query
from pydantic import BaseModel

app = FastAPI()

_BASE_TIME = datetime(2024, 1, 1, tzinfo=timezone.utc)


class Event(BaseModel):
    id: int
    name: str
    updated_at: str


def _seed_events(count: int = 250) -> List[Event]:
    return [
        Event(
            id=i,
            name=f"event-{i}",
            updated_at=(_BASE_TIME + timedelta(minutes=i)).isoformat(),
        )
        for i in range(1, count + 1)
    ]


_EVENTS = _seed_events()


@app.get("/events")
def list_events(
    updated_since: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
):
    records = _EVENTS
    if updated_since:
        cutoff = datetime.fromisoformat(updated_since)
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)
        records = [e for e in records if datetime.fromisoformat(e.updated_at) > cutoff]

    return [event.model_dump() for event in records[:limit]]


def main():
    uvicorn.run(app, host="0.0.0.0", port=8090, log_level="info")


if __name__ == "__main__":
    main()
