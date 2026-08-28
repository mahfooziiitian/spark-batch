"""Unit tests for `rest_ds.rest_api` (APIClient + Paginator hierarchy),
using `requests_mock` instead of a live server. These close the coverage
gap on the class-based ingestion path used by most `examples/`."""

import pytest
import requests

from rest_ds.rest_api import (
    APIClient,
    CursorPaginator,
    LinkHeaderPaginator,
    OffsetPageTokenPaginator,
    OffsetPaginator,
    PageNumberPaginator,
    PaginationFactory,
    read_key_value,
)


def _client(url="https://api.example.com/data", opts=None):
    return APIClient(url, opts or {"authentication": {"type": "none"}})


def test_read_key_value_returns_none_for_missing_path():
    assert read_key_value({"a": {"b": 1}}, "a.c") is None
    assert read_key_value({"a": {"b": 1}}, "a.b") == 1


def test_api_client_get_request(requests_mock):
    requests_mock.get("https://api.example.com/data", json={"ok": True})
    client = _client()
    response = client.make_request()
    assert response.json() == {"ok": True}


def test_api_client_applies_apikey_header(requests_mock):
    requests_mock.get("https://api.example.com/data", json={"ok": True})
    client = _client(
        opts={
            "authentication": {
                "type": "apikey",
                "in": "header",
                "api_key_name": "X-Api-Key",
                "api_key_value": "secret",
            }
        }
    )
    client.make_request()
    assert requests_mock.last_request.headers["X-Api-Key"] == "secret"


def test_api_client_retries_then_succeeds(requests_mock):
    requests_mock.get(
        "https://api.example.com/data",
        [
            {"exc": requests.ConnectionError("boom")},
            {"json": {"ok": True}, "status_code": 200},
        ],
    )
    client = _client(
        opts={
            "authentication": {"type": "none"},
            "retries": {"maxAttempts": 2},
        }
    )
    response = client.make_request()
    assert response.json() == {"ok": True}
    assert requests_mock.call_count == 2


def test_api_client_raises_after_exhausting_retries(requests_mock):
    requests_mock.get(
        "https://api.example.com/data", exc=requests.ConnectionError("down")
    )
    client = _client(
        opts={"authentication": {"type": "none"}, "retries": {"maxAttempts": 2}}
    )
    with pytest.raises(requests.ConnectionError):
        client.make_request()
    assert requests_mock.call_count == 2


def test_offset_paginator(requests_mock):
    requests_mock.get(
        "https://api.example.com/data",
        [
            {"json": [{"id": 1}, {"id": 2}]},
            {"json": [{"id": 3}]},
            {"json": []},
        ],
    )
    client = _client()
    paginator = OffsetPaginator(client, limit=2, limit_key="limit", offset_key="offset")
    result = paginator.paginate()
    assert result == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_offset_paginator_respects_max_pages(requests_mock):
    requests_mock.get(
        "https://api.example.com/data",
        [{"json": [{"id": 1}]}, {"json": [{"id": 2}]}],
    )
    client = _client()
    paginator = OffsetPaginator(
        client,
        limit=1,
        limit_key="limit",
        offset_key="offset",
        max_pages_value=1,
    )
    result = paginator.paginate()
    assert result == [{"id": 1}]


def test_offset_paginator_extracts_result_key(requests_mock):
    requests_mock.get(
        "https://api.example.com/data",
        [
            {"json": {"data": [{"id": 1}]}},
            {"json": {"data": []}},
        ],
    )
    client = _client()
    paginator = OffsetPaginator(
        client, limit=10, limit_key="limit", offset_key="offset", result_key="data"
    )
    assert paginator.paginate() == [{"id": 1}]


def test_offset_page_token_paginator(requests_mock):
    requests_mock.get(
        "https://api.example.com/data",
        [
            {"json": {"items": [{"id": 1}], "next": "tok2"}},
            {"json": {"items": [{"id": 2}], "next": None}},
        ],
    )
    client = _client()
    paginator = OffsetPageTokenPaginator(
        client,
        limit=10,
        limit_key="limit",
        page_token_key="pageToken",
        next_page_token_key="next",
        result_key="items",
    )
    assert paginator.paginate() == [{"id": 1}, {"id": 2}]


def test_page_number_paginator_with_has_next_flag(requests_mock):
    requests_mock.get(
        "https://api.example.com/data",
        [
            {"json": {"data": [{"id": 1}], "hasNext": True}},
            {"json": {"data": [{"id": 2}], "hasNext": False}},
        ],
    )
    client = _client()
    paginator = PageNumberPaginator(
        client,
        page_number_key="page",
        page_size_key="pageSize",
        result_key="data",
        has_next_key="hasNext",
    )
    assert paginator.paginate() == [{"id": 1}, {"id": 2}]


def test_cursor_paginator(requests_mock):
    requests_mock.get(
        "https://api.example.com/data",
        [
            {"json": {"results": [{"id": 1}], "next": "cursor2"}},
            {"json": {"results": [{"id": 2}], "next": None}},
        ],
    )
    client = _client()
    paginator = CursorPaginator(
        client,
        limit_key="limit",
        cursor_key="cursor",
        next_cursor_key="next",
        result_key="results",
    )
    assert paginator.paginate() == [{"id": 1}, {"id": 2}]


def test_link_header_paginator(requests_mock):
    requests_mock.get(
        "https://api.example.com/data",
        json=[{"id": 1}],
        headers={"Link": '<https://api.example.com/data?page=2>; rel="next"'},
    )
    requests_mock.get("https://api.example.com/data?page=2", json=[{"id": 2}])
    client = _client()
    paginator = LinkHeaderPaginator(client)
    assert paginator.paginate() == [{"id": 1}, {"id": 2}]


def test_link_header_paginator_extracts_next_link():
    link_header = '<https://api.example.com/data?page=3>; rel="next"'
    assert (
        LinkHeaderPaginator.extract_next_link(link_header)
        == "https://api.example.com/data?page=3"
    )
    assert LinkHeaderPaginator.extract_next_link("") is None


def test_pagination_factory_returns_expected_types():
    client = _client()
    assert isinstance(
        PaginationFactory.get_paginator(client, "offset"), OffsetPaginator
    )
    assert isinstance(
        PaginationFactory.get_paginator(client, "page"), PageNumberPaginator
    )
    assert isinstance(
        PaginationFactory.get_paginator(client, "cursor"), CursorPaginator
    )
    assert isinstance(
        PaginationFactory.get_paginator(client, "link"), LinkHeaderPaginator
    )
    assert isinstance(
        PaginationFactory.get_paginator(client, "offset_page_token"),
        OffsetPageTokenPaginator,
    )
    with pytest.raises(ValueError):
        PaginationFactory.get_paginator(client, "unsupported")
