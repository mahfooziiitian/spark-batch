from typing import List, Optional

from fastapi import FastAPI, Query
from pydantic import BaseModel

app = FastAPI()


class Item(BaseModel):
    id: int
    name: str


class PaginatedResponse(BaseModel):
    next: Optional[str]
    results: List[Item]


# Simulate 100 items
fake_items = [Item(id=i, name=f"Item {i}") for i in range(1, 101)]


@app.get("/items", response_model=PaginatedResponse)
def get_items(skip: int = Query(0, ge=0), limit: int = Query(10, ge=1, le=50)):
    end = skip + limit
    results = fake_items[skip:end]

    next_url = None
    if end < len(fake_items):
        next_url = f"http://localhost:8000/items?skip={end}&limit={limit}"

    return PaginatedResponse(next=next_url, results=results)
