"""Mock REST API server for the restapi data source examples.

Provides endpoints for:
    - GET /api/users       — returns a list of users (batch read)
    - GET /api/events      — returns events since a given ID (streaming)
    - POST /api/records    — accepts JSON array (batch write)

Run standalone:
    uv run python examples/mock_server/server.py
"""

from __future__ import annotations

from datetime import UTC

import uvicorn
from faker import Faker
from fastapi import FastAPI, Query, Request

app = FastAPI(title="Mock REST API for pyspark-ds-custom")
fake = Faker()
Faker.seed(42)

# Pre-generated users
USERS = [
    {
        "id": i,
        "name": fake.name(),
        "email": fake.email(),
        "city": fake.city(),
        "age": fake.random_int(min=18, max=75),
    }
    for i in range(1, 51)
]

# Pre-generated events (streaming source)
EVENTS = [
    {
        "id": i,
        "event": fake.random_element(["login", "logout", "purchase", "pageview", "signup"]),
        "timestamp": fake.date_time_between(
            start_date="-1d", end_date="now", tzinfo=UTC
        ).isoformat(),
    }
    for i in range(1, 201)
]

# In-memory store for written records
WRITTEN_RECORDS: list[dict] = []


@app.get("/api/users")
def get_users(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    """Return a paginated list of users."""
    subset = USERS[offset : offset + limit]
    return {
        "data": subset,
        "total": len(USERS),
        "limit": limit,
        "offset": offset,
    }


@app.get("/api/users/{user_id}")
def get_user(user_id: int):
    """Return a single user by ID."""
    user = next((u for u in USERS if u["id"] == user_id), None)
    if user is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="User not found")
    return user


@app.get("/api/posts")
def get_posts(
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=25, ge=1, le=100),
):
    """Return a paginated list of posts (page-based, for partition demo)."""
    total_posts = 100
    start = (page - 1) * limit
    posts = [
        {"id": i, "title": f"Post {i}", "author": fake.name(), "views": fake.random_int(10, 5000)}
        for i in range(start + 1, min(start + limit + 1, total_posts + 1))
    ]
    return {
        "data": posts,
        "page": page,
        "pageSize": limit,
        "totalPages": (total_posts + limit - 1) // limit,
        "total": total_posts,
    }


@app.get("/api/events")
def get_events(
    since_id: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
):
    """Return events with id > since_id (for streaming offset tracking)."""
    filtered = [e for e in EVENTS if e["id"] > since_id][:limit]
    return filtered


@app.post("/api/records")
async def write_records(request: Request):
    """Accept a JSON array of records and store them in memory."""
    body = await request.json()
    if isinstance(body, list):
        WRITTEN_RECORDS.extend(body)
        return {"status": "ok", "received": len(body), "total_stored": len(WRITTEN_RECORDS)}
    WRITTEN_RECORDS.append(body)
    return {"status": "ok", "received": 1, "total_stored": len(WRITTEN_RECORDS)}


@app.get("/api/records")
def get_records():
    """Return all written records (for verification)."""
    return {"data": WRITTEN_RECORDS, "total": len(WRITTEN_RECORDS)}


def main() -> None:
    uvicorn.run(app, host="localhost", port=9090, log_level="info")


if __name__ == "__main__":
    main()
