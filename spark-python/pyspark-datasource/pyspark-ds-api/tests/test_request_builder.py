"""Unit tests for `rest_ds.util.request_builder`."""

from requests.auth import HTTPBasicAuth

from rest_ds.util.request_builder import build_request_body, build_request_components


def test_build_request_body_json():
    json_body, form_body = build_request_body({"type": "json", "content": {"a": 1}})
    assert json_body == {"a": 1}
    assert form_body is None


def test_build_request_body_form():
    json_body, form_body = build_request_body({"type": "form", "content": {"a": 1}})
    assert json_body is None
    assert form_body == {"a": 1}


def test_build_request_body_raw():
    json_body, form_body = build_request_body({"type": "raw", "content": "<xml/>"})
    assert json_body == "<xml/>"
    assert form_body is None


def test_build_request_body_empty():
    assert build_request_body({}) == (None, None)
    assert build_request_body(None) == (None, None)


def test_build_request_components_basic_auth_and_headers():
    opts = {
        "authentication": {"type": "basic", "username": "u", "password": "p"},
        "headers": {"X-Custom": "1"},
        "queryParams": {"foo": "bar"},
    }
    headers, auth, json_body, form_body, query_params, cert = build_request_components(
        opts
    )
    assert headers == {"X-Custom": "1"}
    assert isinstance(auth, HTTPBasicAuth)
    assert json_body is None
    assert form_body is None
    assert query_params == {"foo": "bar"}
    assert cert is None


def test_build_request_components_apikey_query_both_conventions():
    opts = {
        "authentication": {"type": "apikey", "in": "query", "name": "k", "value": "v"}
    }
    *_, query_params, _ = build_request_components(opts)
    assert query_params == {"k": "v"}

    opts2 = {
        "authentication": {
            "type": "apikey",
            "in": "query",
            "api_key_name": "k2",
            "api_key_value": "v2",
        }
    }
    *_, query_params2, _ = build_request_components(opts2)
    assert query_params2 == {"k2": "v2"}


def test_build_request_components_mtls_cert():
    opts = {
        "authentication": {
            "type": "mtls",
            "certFile": "/tmp/cert.pem",
            "keyFile": "/tmp/key.pem",
        }
    }
    *_, cert = build_request_components(opts)
    assert cert == ("/tmp/cert.pem", "/tmp/key.pem")
