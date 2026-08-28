"""Unit tests for `rest_ds.util.api_client` (make_request, fetch_data_with_pagination)
and `rest_ds.util.data_processor` (fetch_records, read_api, create_dataframe_json)."""

import pytest
import requests

from rest_ds.util.api_client import fetch_data_with_pagination, make_request
from rest_ds.util.data_processor import create_dataframe_json, fetch_records, read_api


def test_make_request_json(requests_mock):
    requests_mock.get("https://api.example.com/data", json={"ok": True})
    result = make_request(
        "https://api.example.com/data",
        {"method": "GET"},
        headers={},
        auth=None,
        cert=None,
        queryParams={},
        json_body=None,
        form_body=None,
        max_attempts=1,
        responseFormat="json",
    )
    assert result == {"ok": True}


def test_make_request_no_content_returns_empty_dict(requests_mock):
    requests_mock.get("https://api.example.com/data", status_code=204)
    result = make_request(
        "https://api.example.com/data",
        {"method": "GET"},
        headers={},
        auth=None,
        cert=None,
        queryParams={},
        json_body=None,
        form_body=None,
        max_attempts=1,
        responseFormat="json",
    )
    assert result == {}


def test_make_request_retries_then_succeeds(requests_mock):
    requests_mock.get(
        "https://api.example.com/data",
        [{"exc": requests.ConnectionError("boom")}, {"json": {"ok": True}}],
    )
    result = make_request(
        "https://api.example.com/data",
        {"method": "GET"},
        headers={},
        auth=None,
        cert=None,
        queryParams={},
        json_body=None,
        form_body=None,
        max_attempts=2,
        responseFormat="json",
    )
    assert result == {"ok": True}
    assert requests_mock.call_count == 2


def test_make_request_unsupported_format(requests_mock):
    requests_mock.get("https://api.example.com/data", json={"ok": True})
    with pytest.raises(Exception):
        make_request(
            "https://api.example.com/data",
            {"method": "GET"},
            headers={},
            auth=None,
            cert=None,
            queryParams={},
            json_body=None,
            form_body=None,
            max_attempts=1,
            responseFormat="unsupported",
        )


def test_fetch_data_with_pagination():
    calls = []

    def make_request_fn(params):
        calls.append(params)
        skip = params["skip"]
        if skip >= 4:
            return []
        return [{"id": skip}, {"id": skip + 1}]

    result = fetch_data_with_pagination(
        make_request_fn, {"skip": 0, "limit": 4, "pageSize": 2}
    )
    assert result == [{"id": 0}, {"id": 1}, {"id": 2}, {"id": 3}]
    assert len(calls) == 2


def _sample_config(url):
    return {
        "extracts": {
            "extract": {
                "source": {
                    "params": {
                        "location": url,
                        "options": {
                            "method": "GET",
                            "authentication": {"type": "none"},
                        },
                    }
                }
            }
        }
    }


def test_fetch_records_no_pagination(requests_mock):
    requests_mock.get("https://api.example.com/data", json=[{"id": 1}, {"id": 2}])
    config = _sample_config("https://api.example.com/data")
    all_data, opts = fetch_records(config)
    assert all_data == [{"id": 1}, {"id": 2}]
    assert opts["method"] == "GET"


def test_fetch_records_extra_query_params_merged(requests_mock):
    requests_mock.get("https://api.example.com/data", json={"ok": True})
    config = _sample_config("https://api.example.com/data")
    fetch_records(config, extra_query_params={"since": "2024-01-01"})
    assert requests_mock.last_request.qs.get("since") == ["2024-01-01"]


def test_read_api_returns_dataframe(spark, requests_mock):
    requests_mock.get("https://api.example.com/data", json=[{"id": 1}, {"id": 2}])
    config = _sample_config("https://api.example.com/data")
    df = read_api(spark, config)
    assert sorted(row["id"] for row in df.collect()) == [1, 2]


def test_create_dataframe_json_empty_data_returns_empty_dataframe(spark):
    df = create_dataframe_json(spark, [])
    assert df.count() == 0
