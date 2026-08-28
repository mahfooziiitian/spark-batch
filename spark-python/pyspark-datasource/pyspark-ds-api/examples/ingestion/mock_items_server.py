"""Mock paginated `/items` API used by the ingestion examples in this folder.

Serves ``GET /items?page=<n>`` returning a page of ``{"id", "name"}`` records
plus a ``next`` field (a full URL to the next page, or ``null`` on the last
page) so both page-number-based scripts (``parallel_ingestion*.py``,
``parallel_with_spark_partitions.py``) and link-following scripts
(``pyspark_rest_optimized.py``) can be demonstrated against the same server.

Run standalone:

    PYTHONPATH=src uv run python examples/ingestion/mock_items_server.py
"""

import uvicorn
from fastapi import FastAPI, HTTPException, Query

app = FastAPI()

TOTAL_PAGES = 5
PAGE_SIZE = 10


@app.get("/items")
def get_items(page: int = Query(1, ge=1)):
    if page > TOTAL_PAGES:
        raise HTTPException(status_code=404, detail="Page not found")

    start = (page - 1) * PAGE_SIZE
    results = [
        {"id": start + i + 1, "name": f"Item {start + i + 1}"} for i in range(PAGE_SIZE)
    ]
    next_url = (
        f"http://localhost:8091/items?page={page + 1}" if page < TOTAL_PAGES else None
    )
    return {
        "results": results,
        "page": page,
        "total_pages": TOTAL_PAGES,
        "next": next_url,
    }


def main() -> None:
    uvicorn.run(app, host="localhost", port=8091, log_level="info")


if __name__ == "__main__":
    main()
