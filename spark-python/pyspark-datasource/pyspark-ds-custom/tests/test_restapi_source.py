"""Tests for RestApiDataSource (batch reader) using a real threaded HTTP server."""

from __future__ import annotations

import threading
import time

import pytest
import uvicorn
from fastapi import FastAPI
from pyspark.sql import SparkSession

from custom_ds.restapi import RestApiDataSource

pytestmark = pytest.mark.pyspark

# ---------------------------------------------------------------------------
# Lightweight test server
# ---------------------------------------------------------------------------

_test_app = FastAPI()

MOCK_USERS = [
    {"id": 1, "name": "Alice", "email": "alice@test.com", "age": 30},
    {"id": 2, "name": "Bob", "email": "bob@test.com", "age": 45},
    {"id": 3, "name": "Charlie", "email": "charlie@test.com", "age": 28},
]


@_test_app.get("/users")
def get_users():
    return MOCK_USERS


@_test_app.get("/nested")
def get_nested():
    return {"data": MOCK_USERS, "total": len(MOCK_USERS)}


@_test_app.get("/empty")
def get_empty():
    return []


@_test_app.get("/users/{user_id}")
def get_user(user_id: int):
    user = next((u for u in MOCK_USERS if u["id"] == user_id), None)
    return user if user else {}


@_test_app.get("/posts")
def get_posts(page: int = 1, limit: int = 5):
    """Page-based endpoint for partitioning tests."""
    all_posts = [{"id": i, "title": f"Post {i}"} for i in range(1, 21)]
    start = (page - 1) * limit
    return {"data": all_posts[start : start + limit]}


_TEST_PORT = 19091


@pytest.fixture(scope="module", autouse=True)
def _start_test_server():
    """Start a real HTTP server in a daemon thread for the test module."""
    config = uvicorn.Config(_test_app, host="127.0.0.1", port=_TEST_PORT, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    # Wait for server readiness
    import requests

    for _ in range(20):
        try:
            requests.get(f"http://127.0.0.1:{_TEST_PORT}/users", timeout=1)
            break
        except requests.ConnectionError:
            time.sleep(0.25)
    yield
    server.should_exit = True
    thread.join(timeout=3)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_restapi_batch_read_top_level_array(spark: SparkSession) -> None:
    spark.dataSource.register(RestApiDataSource)

    df = (
        spark.read.format("restapi")
        .option("url", f"http://127.0.0.1:{_TEST_PORT}/users")
        .option("schema", "id LONG, name STRING, email STRING, age LONG")
        .load()
    )

    assert df.count() == 3
    rows = sorted(df.collect(), key=lambda r: r["id"])
    assert rows[0]["name"] == "Alice"
    assert rows[2]["age"] == 28


def test_restapi_batch_read_nested_result_key(spark: SparkSession) -> None:
    spark.dataSource.register(RestApiDataSource)

    df = (
        spark.read.format("restapi")
        .option("url", f"http://127.0.0.1:{_TEST_PORT}/nested")
        .option("resultKey", "data")
        .option("schema", "id LONG, name STRING, email STRING, age LONG")
        .load()
    )

    assert df.count() == 3


def test_restapi_batch_read_empty_response(spark: SparkSession) -> None:
    spark.dataSource.register(RestApiDataSource)

    df = (
        spark.read.format("restapi")
        .option("url", f"http://127.0.0.1:{_TEST_PORT}/empty")
        .option("schema", "id LONG, name STRING")
        .load()
    )

    assert df.count() == 0


# ---------------------------------------------------------------------------
# Partitioning strategy tests
# ---------------------------------------------------------------------------


def test_restapi_url_based_partitioning(spark: SparkSession) -> None:
    spark.dataSource.register(RestApiDataSource)

    urls = ",".join(f"http://127.0.0.1:{_TEST_PORT}/users/{i}" for i in range(1, 4))

    df = (
        spark.read.format("restapi")
        .option("partitionStrategy", "urls")
        .option("urls", urls)
        .option("schema", "id LONG, name STRING, email STRING, age LONG")
        .load()
    )

    assert df.count() == 3
    assert df.rdd.getNumPartitions() == 3


def test_restapi_page_based_partitioning(spark: SparkSession) -> None:
    spark.dataSource.register(RestApiDataSource)

    df = (
        spark.read.format("restapi")
        .option("partitionStrategy", "pages")
        .option("url", f"http://127.0.0.1:{_TEST_PORT}/posts")
        .option("totalPages", "4")
        .option("pageSize", "5")
        .option("resultKey", "data")
        .option("schema", "id LONG, title STRING")
        .load()
    )

    assert df.count() == 20
    assert df.rdd.getNumPartitions() == 4
