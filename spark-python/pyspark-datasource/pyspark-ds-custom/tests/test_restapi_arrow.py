"""Tests for RestApiArrowDataSource (Arrow batch reader) using a real threaded HTTP server."""

from __future__ import annotations

import threading
import time

import pytest
import uvicorn
from fastapi import FastAPI
from pyspark.sql import SparkSession

from custom_ds.restapi import RestApiArrowDataSource

pytestmark = pytest.mark.pyspark

_arrow_app = FastAPI()

MOCK_DATA = [
    {"id": 1, "name": "Alice", "score": 95},
    {"id": 2, "name": "Bob", "score": 87},
    {"id": 3, "name": "Charlie", "score": 92},
    {"id": 4, "name": "Diana", "score": 88},
    {"id": 5, "name": "Eve", "score": 91},
]


@_arrow_app.get("/data")
def get_data():
    return {"results": MOCK_DATA}


_ARROW_PORT = 19093


@pytest.fixture(scope="module", autouse=True)
def _start_arrow_server():
    config = uvicorn.Config(_arrow_app, host="127.0.0.1", port=_ARROW_PORT, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    import requests

    for _ in range(20):
        try:
            requests.get(f"http://127.0.0.1:{_ARROW_PORT}/data", timeout=1)
            break
        except requests.ConnectionError:
            time.sleep(0.25)
    yield
    server.should_exit = True
    thread.join(timeout=3)


def test_arrow_reader_returns_correct_data(spark: SparkSession) -> None:
    spark.dataSource.register(RestApiArrowDataSource)

    df = (
        spark.read.format("restapi_arrow")
        .option("url", f"http://127.0.0.1:{_ARROW_PORT}/data")
        .option("resultKey", "results")
        .option("schema", "id LONG, name STRING, score LONG")
        .load()
    )

    assert df.count() == 5
    rows = sorted(df.collect(), key=lambda r: r["id"])
    assert rows[0]["name"] == "Alice"
    assert rows[4]["score"] == 91
