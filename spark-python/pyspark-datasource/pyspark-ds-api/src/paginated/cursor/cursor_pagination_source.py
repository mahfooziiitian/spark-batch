from typing import Optional, List
from fastapi import FastAPI, Query
from sqlmodel import SQLModel, Field, Session, create_engine, select
from pydantic import BaseModel
import uvicorn

# ─── Models ────────────────────────────────────────────────────────────


class Item(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    created_at: int


# ─── DB Setup ───────────────────────────────────────────────────────────

sqlite_file_name = "db_cursor.sqlite"
engine = create_engine(f"sqlite:///{sqlite_file_name}", echo=False)


def seed_data():
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        if session.exec(select(Item)).first():
            return
        for i in range(1, 101):
            session.add(Item(name=f"Item {i}", created_at=i))
        session.commit()


seed_data()

# ─── Response Schema ───────────────────────────────────────────────────


class CursorPage(BaseModel):
    data: List[Item]
    next_cursor: Optional[int] = None
    limit: int


# ─── FastAPI App ───────────────────────────────────────────────────────

app = FastAPI()


@app.get("/items", response_model=CursorPage)
def get_items(
    limit: int = Query(10, ge=1, le=100), cursor: Optional[int] = Query(None)
):
    with Session(engine) as session:
        query = select(Item).order_by(Item.created_at)
        if cursor:
            query = query.where(Item.created_at > cursor)

        results = session.exec(query.limit(limit + 1)).all()

        next_cursor = None
        if len(results) > limit:
            next_cursor = results[-1].created_at
            results = results[:limit]

        return CursorPage(data=results, next_cursor=next_cursor, limit=limit)


def main():

    uvicorn.run(app, host="localhost", port=8081, log_level="info")


if __name__ == "__main__":
    main()
