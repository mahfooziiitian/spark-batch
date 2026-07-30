#!/usr/bin/env python3
"""Generate all JSON fixture files needed by the examples.

Run this script once before running examples:
    python generate_test_data.py

Creates files under data/file_data/json/ (relative to this script's location).
"""

import json
import os
from pathlib import Path

BASE = Path(os.environ.get("DATA_HOME")) / "file_data" / "json"

if BASE is None:
    BASE = Path(__file__).parent / "data" / "file_data" / "json"


def _write_lines(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def _write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def generate_corrupt_record() -> None:
    """Corrupt record data — used by permissive, drop_malformed, failfast, rescued_data."""
    lines = [
        '{"name": "John", "age": 30}',
        '{"name": "Alice", "age": "twenty-five"}',
        '{"name": "Bob", "age": 35}',
    ]
    for name in [
        "corrupt_record_permissive",
        "corrupt_record_drop_malformed",
        "corrupt_record_failfast",
        "rescue_data_column",
    ]:
        _write_lines(BASE / "properties" / "corrupt_record" / f"{name}.json", lines)


def generate_comment() -> None:
    """JSON with comments."""
    content = """\
[
  // This is a single-line comment
  {"name": "Alice", "age": 30, "city": "New York"},
  /* This is a
     multi-line comment */
  {"name": "Bob", "age": 25, "city": "San Francisco"},
  {"name": "Charlie", "age": 35, "city": "Chicago"}
]
"""
    path = BASE / "properties" / "comment" / "comment.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)


def generate_encoding() -> None:
    """UTF-8, UTF-16 (BE/LE with BOM), and UTF-32 encoded JSON files."""
    data = [
        {"name": "Alice", "city": "New York", "age": 30},
        {"name": "böb", "city": "München", "age": 25},
        {"name": "田中", "city": "東京", "age": 40},
    ]
    lines = "\n".join(json.dumps(r) for r in data) + "\n"

    # UTF-8
    path = BASE / "properties" / "encoding" / "utf_8" / "utf_8.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(lines.encode("utf-8"))

    # UTF-16 BE with BOM
    path = BASE / "properties" / "encoding" / "utf_16" / "utf_16_be_bom.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\xfe\xff" + lines.encode("utf-16-be"))

    # UTF-16 LE with BOM
    path = BASE / "properties" / "encoding" / "utf_16" / "utf_16_le_bom.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\xff\xfe" + lines.encode("utf-16-le"))

    # UTF-32
    path = BASE / "properties" / "encoding" / "utf_32" / "utf_32.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(lines.encode("utf-32"))


def generate_primitives_as_string() -> None:
    data = [
        {"name": "Alice", "age": 30, "score": 95.5, "active": True},
        {"name": "Bob", "age": 25, "score": 88.0, "active": False},
    ]
    _write_json(BASE / "properties" / "primitives_as_string" / "primitives_as_string.json", data)


def generate_numbers() -> None:
    """JSON with NaN, Infinity, -Infinity."""
    lines = [
        '{"sensor": "temp_1", "value": NaN}',
        '{"sensor": "temp_2", "value": Infinity}',
        '{"sensor": "temp_3", "value": -Infinity}',
        '{"sensor": "temp_4", "value": 23.5}',
    ]
    path = BASE / "properties" / "numbers" / "allow_non_numeric_numbers.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[\n" + ",\n".join(f"  {line}" for line in lines) + "\n]\n")


def generate_leading_zero() -> None:
    lines = [
        '{"id": "001", "zipcode": 00704, "name": "Alice"}',
        '{"id": "002", "zipcode": 010001, "name": "Bob"}',
    ]
    path = BASE / "properties" / "leading_zero" / "leading_zero_numerical.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[\n" + ",\n".join(f"  {line}" for line in lines) + "\n]\n")


def generate_localization() -> None:
    data = [
        {"name": "Alice", "date_of_birth": "15-Jan-1990", "salary": "1,250.50"},
        {"name": "Bob", "date_of_birth": "22-Mar-1985", "salary": "2,100.00"},
    ]
    _write_json(BASE / "properties" / "localization" / "us_locale.json", data)


def generate_prefers_decimal() -> None:
    data = [
        {"product": "Widget", "price": 19.99, "quantity": 100},
        {"product": "Gadget", "price": 149.95, "quantity": 50},
    ]
    _write_json(BASE / "properties" / "prefer_decimal" / "prefer_decimal.json", data)


def generate_line_separator() -> None:
    """Line separator examples."""
    lines = [
        '{"name": "Alice", "age": 30}',
        '{"name": "Bob", "age": 25}',
        '{"name": "Charlie", "age": 35}',
    ]
    # \n separator
    path = BASE / "properties" / "line_seperator" / "line_seperator.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")

    # \r\n separator
    path = BASE / "properties" / "line_seperator" / "line_seperator_r_n.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\r\n".join(lines) + "\r\n")


def generate_timestamp_formats() -> None:
    """Timestamp-related JSON files."""
    # Standard timestamp
    data = [
        {"event": "login", "timestamp": "2024-01-15T10:30:00.000"},
        {"event": "logout", "timestamp": "2024-01-15T18:45:30.500"},
    ]
    _write_json(BASE / "properties" / "timestamp_format" / "timestamp_format.json", data)

    # Timestamp NTZ
    data = [
        {"event": "start", "ts": "2024-03-15T08:00:00"},
        {"event": "end", "ts": "2024-03-15T17:30:00"},
    ]
    _write_json(BASE / "properties" / "timestamp_format" / "timestamp_ntz_format.json", data)

    # Timezone format
    data = [
        {"event": "meeting", "ts": "2024-01-15T10:30:00+05:30"},
        {"event": "call", "ts": "2024-01-15T14:00:00-05:00"},
    ]
    _write_json(BASE / "properties" / "timestamp_format" / "timezone_format.json", data)

    # Date time parsing fallback
    data = [
        {"event": "signup", "date": "2024-01-15", "time": "10:30:00"},
        {"event": "login", "date": "Jan 15, 2024", "time": "2:30 PM"},
    ]
    _write_json(BASE / "properties" / "timestamp_format" / "date_time_parsing_fallback.json", data)


def generate_quoting() -> None:
    """Single quoting and unquoted field names."""
    # Single quoting
    lines = [
        "{'name': 'Alice', 'age': 30}",
        "{'name': 'Bob', 'age': 25}",
    ]
    path = BASE / "properties" / "quoting" / "single_quoting.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[\n" + ",\n".join(f"  {line}" for line in lines) + "\n]\n")

    # Unquoted field names
    lines = [
        "{name: \"Alice\", age: 30}",
        "{name: \"Bob\", age: 25}",
    ]
    path = BASE / "properties" / "unquote_field_names" / "unquote_field_name.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("[\n" + ",\n".join(f"  {line}" for line in lines) + "\n]\n")


def generate_sampling_ratio() -> None:
    """Large-ish dataset for sampling ratio demo."""
    data = [{"id": i, "value": f"item_{i}", "score": i * 1.5} for i in range(100)]
    _write_json(BASE / "properties" / "sampling_ratio" / "sampling_ratio.json", data)


def generate_escape_characters() -> None:
    lines = [
        '{"text": "line1\\nline2", "path": "C:\\\\Users\\\\alice"}',
        '{"text": "tab\\there", "path": "/home/bob"}',
    ]
    _write_lines(BASE / "escape_characters" / "escape_characters.json", lines)


def generate_null_fields() -> None:
    """Fields with null values."""
    data = [
        {"name": "Alice", "age": 30, "email": "alice@example.com"},
        {"name": "Bob", "age": None, "email": None},
        {"name": "Charlie", "age": 35, "email": "charlie@example.com"},
    ]
    _write_json(BASE / "properties" / "fields" / "drop_field_if_all_null.json", data)

    data = [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Bob", "age": None, "city": None},
        {"name": "Charlie", "age": 35, "city": "Chicago"},
    ]
    _write_json(BASE / "properties" / "fields" / "ignore_null_fields.json", data)


def generate_dynamic_keys() -> None:
    """Variable/polymorphic key JSON files."""
    data = {
        "Items": {
            "category_a": [
                {"id": "1", "name": "Widget", "val": "100"},
                {"id": "2", "name": "Gadget", "val": "200"},
            ],
            "category_b": [
                {"id": "3", "name": "Doohickey", "val": "300"},
            ],
        }
    }
    _write_json(BASE / "dynamic_keys" / "dynamic_keys.json", data)

    data = {
        "type": "user_event",
        "payload": {"user_id": "u123", "action": "click", "target": "button_1"},
    }
    _write_json(BASE / "dynamic_keys" / "polymorphic.json", [
        {"type": "user_event", "payload": {"user_id": "u123", "action": "click"}},
        {"type": "system_event", "payload": {"service": "auth", "status": "healthy"}},
    ])


def main() -> None:
    print(f"Generating test data in: {BASE}")
    generate_corrupt_record()
    generate_comment()
    generate_encoding()
    generate_primitives_as_string()
    generate_numbers()
    generate_leading_zero()
    generate_localization()
    generate_prefers_decimal()
    generate_line_separator()
    generate_timestamp_formats()
    generate_quoting()
    generate_sampling_ratio()
    generate_escape_characters()
    generate_null_fields()
    generate_dynamic_keys()

    file_count = sum(1 for _ in BASE.rglob("*.json"))
    print(f"Generated {file_count} JSON fixture files.")


if __name__ == "__main__":
    main()
