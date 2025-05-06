from typing import List

from fastapi import FastAPI, Query
from pydantic import BaseModel

app = FastAPI()


# Response models
class Item(BaseModel):
    id: int
    name: str


# Simulated dataset (e.g., 500 records)
ALL_DATA = [{"id": int(i), "name": str(f"Item {i}")} for i in range(1, 501)]


class PaginatedResponse(BaseModel):
    page: int
    page_size: int
    results: List[Item]


@app.get("/items", response_model=PaginatedResponse)
def get_items(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=100)):
    start = (page - 1) * page_size
    end = start + page_size
    return PaginatedResponse(
        page=page,
        page_size=page_size,
        results=[Item(**data) for data in ALL_DATA[start:end]],
    )
