from typing import List

from fastapi import FastAPI, Query
from pydantic import BaseModel

app = FastAPI()

# Dummy data
fake_items = [{"id": i, "name": f"Item {i}"} for i in range(1, 101)]


class Item(BaseModel):
    id: int
    name: str


class PaginatedItems(BaseModel):
    total: int
    skip: int
    limit: int
    results: List[Item]


# @app.get("/items/", response_model=PaginatedItems)
# def get_items(skip: int = Query(0, ge=0), limit: int = Query(10, ge=1, le=100)):
#     paginated_data = fake_items[skip : skip + limit]
#     return PaginatedItems(total=len(fake_items), skip=skip, limit=limit, results=paginated_data)
