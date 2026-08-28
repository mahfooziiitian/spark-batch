import math
from typing import List, Optional

import uvicorn
from fastapi import FastAPI, Query, Request
from pydantic import BaseModel
from sqlmodel import Field, Session, SQLModel, create_engine, func, select

# ─── Database Setup ────────────────────────────────────────────────


class Item(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    category: Optional[str] = None
    created_at: Optional[int] = None  # use integer timestamps for simplicity


sqlite_file_name = "database.db"
engine = create_engine(f"sqlite:///{sqlite_file_name}", echo=False)


def create_db_and_seed():
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        if not session.exec(select(func.count(Item.id))).one():
            for i in range(1, 201):
                session.add(
                    Item(
                        name=f"Item {i}",
                        category="A" if i % 2 == 0 else "B",
                        created_at=200 - i,
                    )
                )
            session.commit()


create_db_and_seed()

# ─── Response Models ───────────────────────────────────────────────


class PaginationMeta(BaseModel):
    page: int
    page_size: int
    total: Optional[int] = None
    total_pages: Optional[int] = None
    has_next: Optional[bool] = None
    has_previous: Optional[bool] = None
    pages: Optional[List[int]] = None


class PaginationLinks(BaseModel):
    self: str
    next: Optional[str] = None
    prev: Optional[str] = None
    first: Optional[str] = None
    last: Optional[str] = None


class PaginatedResponse(BaseModel):
    data: List[Item]
    meta: PaginationMeta
    links: Optional[PaginationLinks] = None


# ─── FastAPI App ───────────────────────────────────────────────────

app = FastAPI()


@app.get("/items", response_model=PaginatedResponse)
def get_items(
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    include_total: bool = Query(True),
    category: Optional[str] = None,
    order: Optional[str] = Query("asc"),
):
    offset = (page - 1) * page_size
    with Session(engine) as session:
        query = select(Item)
        if category:
            query = query.where(Item.category == category)

        query = query.order_by(
            Item.created_at.desc() if order == "desc" else Item.created_at.asc()
        )
        items = session.exec(query.offset(offset).limit(page_size + 1)).all()

        total = None
        total_pages = None
        has_next = len(items) > page_size
        has_previous = page > 1
        if has_next:
            items = items[:page_size]

        if include_total:
            count_query = select(func.count()).select_from(Item)
            if category:
                count_query = count_query.where(Item.category == category)
            total = session.exec(count_query).one()
            total_pages = math.ceil(total / page_size)

        # UI window: [page-2, page-1, page, page+1, page+2]
        pages_window = None
        if total_pages:
            start = max(1, page - 2)
            end = min(total_pages, page + 2)
            pages_window = list(range(start, end + 1))

        # Build navigation links
        def build_url(p: int) -> str:
            return str(request.url.include_query_params(page=p, page_size=page_size))

        links = PaginationLinks(
            self=build_url(page),
            next=build_url(page + 1) if has_next else None,
            prev=build_url(page - 1) if has_previous else None,
            first=build_url(1),
            last=build_url(total_pages) if total_pages else None,
        )

        return PaginatedResponse(
            data=items,
            meta=PaginationMeta(
                page=page,
                page_size=page_size,
                total=total,
                total_pages=total_pages,
                has_next=has_next,
                has_previous=has_previous,
                pages=pages_window,
            ),
            links=links,
        )


def main():

    uvicorn.run(app, host="localhost", port=8081, log_level="info")


if __name__ == "__main__":
    main()
