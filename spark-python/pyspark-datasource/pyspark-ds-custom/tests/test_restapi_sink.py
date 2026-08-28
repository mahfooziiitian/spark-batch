"""Tests for RestApiSinkDataSource (batch writer) using a real threaded HTTP server."""

from __future__ import annotations

import threading
import time

import pytest
import uvicorn
from fastapi import FastAPI, Request
from pyspark.sql import SparkSession

from custom_ds.restapi import RestApiSinkDataSource

# ---------------------------------------------------------------------------
# Lightweight test server for writes
# ---------------------------------------------------------------------------

_write_app = FastAPI()
_received_batches: list[list[dict]] = []


@_write_app.post("/write")
async def receive_records(request: Request):
    body = await request.json()
    _received_batches.append(body)
    return {"status": "ok", "received": len(body)}


_WRITE_PORT = 19092


@pytest.fixture(scope="module", autouse=True)
def _start_write_server():
    """Start a real HTTP server in a daemon thread for write tests."""
    config = uvicorn.Config(_write_app, host="127.0.0.1", port=_WRITE_PORT, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    import requests

    for _ in range(20):
        try:
            requests.post(f"http://127.0.0.1:{_WRITE_PORT}/write", json=[], timeout=1)
            break
        except requests.ConnectionError:
            time.sleep(0.25)
    _received_batches.clear()
    yield
    server.should_exit = True
    thread.join(timeout=3)


@pytest.fixture(autouse=True)
def _clear_batches():
    _received_batches.clear()
    yield


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_restapi_sink_posts_rows(spark: SparkSession) -> None:
    spark.dataSource.register(RestApiSinkDataSource)

    df = spark.range(5).selectExpr("id", "concat('val-', id) as value").coalesce(1)

    df.write.format("restapi_sink").option("url", f"http://127.0.0.1:{_WRITE_PORT}/write").option(
        "batchSize", "100"
    ).mode("append").save()

    total_rows = sum(len(batch) for batch in _received_batches)
    assert total_rows == 5


def test_restapi_sink_batches_requests(spark: SparkSession) -> None:
    spark.dataSource.register(RestApiSinkDataSource)

    df = spark.range(10).selectExpr("id", "concat('val-', id) as value").coalesce(1)

    df.write.format("restapi_sink").option("url", f"http://127.0.0.1:{_WRITE_PORT}/write").option(
        "batchSize", "3"
    ).mode("append").save()

    # 10 rows / batch_size 3 = 4 requests (3+3+3+1)
    assert len(_received_batches) == 4
    total_rows = sum(len(batch) for batch in _received_batches)
    assert total_rows == 10
