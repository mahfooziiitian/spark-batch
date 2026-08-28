import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional

import uvicorn
from fastapi import Depends, FastAPI, Query
from sqlalchemy import func
from sqlmodel import Field, Session, SQLModel, create_engine, select


# SQLModel model
class Item(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str


# Pydantic models
class ItemRead(SQLModel):
    id: int
    name: str


class PaginatedResponse(SQLModel):
    items: List[ItemRead]
    total_items: int
    limit: int
    offset: int
    next_offset: Optional[int]


# DB setup
DATA_DIR = Path(os.environ.get("DATA_HOME", "/tmp")) / "rest_api_ds"
DATA_DIR.mkdir(parents=True, exist_ok=True)
sqlite_url = f"sqlite:///{DATA_DIR / 'database.db'}"
engine = create_engine(sqlite_url, echo=False)


def get_session():
    with Session(engine) as session:
        yield session


@asynccontextmanager
async def lifespan(app: FastAPI):
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        if not session.exec(select(Item)).first():
            for i in range(100):
                session.add(Item(name=f"Item {i + 1}"))
            session.commit()
    yield


app = FastAPI(lifespan=lifespan)


# Optional: Create items
@app.post("/items/create")
def create_items(count: int = 100, session: Session = Depends(get_session)):
    for i in range(count):
        session.add(Item(name=f"Item {i+1}"))
    session.commit()
    return {"message": f"{count} items created"}


# 🧩 FIXED: Pagination with proper count query
@app.get("/items", response_model=PaginatedResponse)
def get_items(
    limit: int = Query(10, gt=0, le=100),
    offset: int = Query(0, ge=0),
    session: Session = Depends(get_session),
):
    # Proper total count using SQL function
    total = session.exec(select(func.count()).select_from(Item)).one()
    items = session.exec(select(Item).offset(offset).limit(limit)).all()
    next_offset = offset + limit if offset + limit < total else None

    return {
        "items": items,
        "total_items": total,
        "limit": limit,
        "offset": offset,
        "next_offset": next_offset,
    }


def main():

    uvicorn.run(app, host="localhost", port=8081, log_level="info")


if __name__ == "__main__":
    main()
