import base64
from typing import Optional, List
from fastapi import FastAPI, Query, Request
from sqlmodel import Field, SQLModel, select, create_engine, Session
from pydantic import BaseModel
import uvicorn

# ─── DB Models ─────────────────────────────────────────────────────


class Item(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    created_at: int  # just an int timestamp


sqlite_file_name = "db_token_offset.db"
engine = create_engine(f"sqlite:///{sqlite_file_name}")


def create_db_and_seed():
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        count = session.exec(select(Item)).all()
        if not count:
            for i in range(100):
                session.add(Item(name=f"Item {i+1}", created_at=i))
            session.commit()


create_db_and_seed()

# ─── Utils ─────────────────────────────────────────────────────────


def encode_token(offset: int) -> str:
    return base64.urlsafe_b64encode(str(offset).encode()).decode()


def decode_token(token: str) -> int:
    try:
        return int(base64.urlsafe_b64decode(token.encode()).decode())
    except Exception:
        return 0  # fallback if malformed token


# ─── Response Models ───────────────────────────────────────────────


class PaginatedTokenResponse(BaseModel):
    data: List[Item]
    next_page_token: Optional[str] = None
    prev_page_token: Optional[str] = None
    limit: int


# ─── FastAPI Endpoint ──────────────────────────────────────────────

app = FastAPI()


@app.get("/items", response_model=PaginatedTokenResponse)
def get_items(
    request: Request,
    limit: int = Query(10, ge=1, le=100),
    page_token: Optional[str] = Query(None),
):
    offset = decode_token(page_token) if page_token else 0

    with Session(engine) as session:
        items = session.exec(
            select(Item).order_by(Item.created_at).offset(offset).limit(limit + 1)
        ).all()

        has_next = len(items) > limit
        if has_next:
            items = items[:limit]

        next_offset = offset + limit
        prev_offset = max(offset - limit, 0)

        return PaginatedTokenResponse(
            data=items,
            limit=limit,
            next_page_token=encode_token(next_offset) if has_next else None,
            prev_page_token=encode_token(prev_offset) if offset > 0 else None,
        )


def main():

    uvicorn.run(app, host="localhost", port=8081, log_level="info")


if __name__ == "__main__":
    main()
