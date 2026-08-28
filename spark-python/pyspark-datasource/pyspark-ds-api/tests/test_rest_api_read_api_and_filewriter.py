"""Unit tests for `rest_ds.rest_api.FileWriter` and the module-level
`read_api()` entry point (the file-writing ingestion path used by most
`examples/`), plus `rest_ds.util.config_loader`."""

import json

from rest_ds.rest_api import FileWriter, read_api
from rest_ds.util.config_loader import load_config


def test_load_config_reads_yaml(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "extracts:\n"
        "  extract:\n"
        "    source:\n"
        "      params:\n"
        "        location: https://api.example.com/data\n"
        "        options:\n"
        "          authentication:\n"
        "            type: none\n"
    )
    config = load_config(config_path)
    params = config["extracts"]["extract"]["source"]["params"]
    assert params["location"] == "https://api.example.com/data"
    assert params["options"]["authentication"]["type"] == "none"


def test_write_json_response_to_file(tmp_path):
    filepath = tmp_path / "out.json"
    FileWriter.write_json_response_to_file([{"a": 1}, {"b": 2}], filepath=str(filepath))
    lines = filepath.read_text().splitlines()
    assert json.loads(lines[0]) == {"a": 1}
    assert json.loads(lines[1]) == {"b": 2}


def test_append_json_pages_to_file_handles_list_and_dict_pages(tmp_path):
    filepath = tmp_path / "pages.jsonl"
    FileWriter.append_json_pages_to_file(
        [[{"a": 1}, {"a": 2}], {"b": 3}], filepath=str(filepath)
    )
    lines = filepath.read_text().splitlines()
    assert [json.loads(line) for line in lines] == [{"a": 1}, {"a": 2}, {"b": 3}]


def test_write_text_response_to_file(tmp_path):
    class _FakeResponse:
        text = "hello world"

    filepath = tmp_path / "out.txt"
    FileWriter.write_text_response_to_file(_FakeResponse(), filepath=str(filepath))
    assert filepath.read_text() == "hello world"


def _sample_config(url, **opts_overrides):
    opts = {"method": "GET", "authentication": {"type": "none"}}
    opts.update(opts_overrides)
    return {
        "extracts": {
            "extract": {"source": {"params": {"location": url, "options": opts}}}
        }
    }


def test_read_api_writes_json_file(requests_mock, tmp_path, monkeypatch):
    requests_mock.get("https://api.example.com/data", json=[{"id": 1}, {"id": 2}])
    monkeypatch.chdir(tmp_path)
    config = _sample_config(
        "https://api.example.com/data", filepath=str(tmp_path / "out")
    )
    read_api(spark=None, config=config)
    output_file = tmp_path / "out.json"
    assert output_file.exists()
    records = [json.loads(line) for line in output_file.read_text().splitlines()]
    assert records == [{"id": 1}, {"id": 2}]


def test_read_api_with_pagination(requests_mock, tmp_path):
    requests_mock.get(
        "https://api.example.com/data",
        [{"json": [{"id": 1}]}, {"json": []}],
    )
    config = _sample_config(
        "https://api.example.com/data",
        filepath=str(tmp_path / "out"),
        pagination={
            "strategy": "offset",
            "limit": 10,
            "limit_key": "limit",
            "offset_key": "offset",
        },
    )
    read_api(spark=None, config=config)
    output_file = tmp_path / "out.json"
    records = [json.loads(line) for line in output_file.read_text().splitlines()]
    assert records == [{"id": 1}]
