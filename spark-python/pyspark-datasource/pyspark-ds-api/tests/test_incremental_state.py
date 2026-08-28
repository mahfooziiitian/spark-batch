import json

import pytest

from rest_ds.incremental.state_store import IncrementalStateStore
from rest_ds.incremental.watermark import (
    apply_lookback,
    compute_next_watermark,
    format_value,
    parse_iso8601_duration,
    parse_value,
)


class TestWatermarkParsing:
    """Tests for parsing/formatting/lookback of watermark values."""

    def test_parse_and_format_datetime_roundtrip(self):
        value = "2024-01-01T00:05:00+00:00"
        parsed = parse_value(value, "datetime")
        assert format_value(parsed, "datetime") == value

    def test_parse_and_format_datetime_with_custom_format(self):
        value = "2024-01-01 00:05:00"
        fmt = "%Y-%m-%d %H:%M:%S"
        parsed = parse_value(value, "datetime", fmt)
        assert format_value(parsed, "datetime", fmt) == value

    def test_parse_integer(self):
        assert parse_value("42", "integer") == 42

    def test_parse_string_is_passthrough(self):
        assert parse_value("cursor-abc", "string") == "cursor-abc"

    def test_parse_iso8601_duration_minutes(self):
        assert parse_iso8601_duration("PT15M").total_seconds() == 15 * 60

    def test_parse_iso8601_duration_days_and_hours(self):
        duration = parse_iso8601_duration("P1DT2H")
        assert duration.total_seconds() == (24 + 2) * 3600

    def test_parse_iso8601_duration_invalid_raises(self):
        with pytest.raises(ValueError):
            parse_iso8601_duration("not-a-duration")


class TestLookback:
    """Tests for the lookback/overlap window applied before each run."""

    def test_lookback_rewinds_datetime_watermark(self):
        result = apply_lookback("2024-01-01T00:10:00+00:00", "PT5M", "datetime")
        assert result == "2024-01-01T00:05:00+00:00"

    def test_lookback_noop_when_not_configured(self):
        result = apply_lookback("2024-01-01T00:10:00+00:00", None, "datetime")
        assert result == "2024-01-01T00:10:00+00:00"

    def test_lookback_noop_for_non_datetime_types(self):
        result = apply_lookback("42", "PT5M", "integer")
        assert result == "42"


class TestComputeNextWatermark:
    """Tests for scanning fetched records to derive the next watermark."""

    def test_computes_max_datetime_value(self):
        records = [
            {"id": 1, "updated_at": "2024-01-01T00:01:00+00:00"},
            {"id": 2, "updated_at": "2024-01-01T00:05:00+00:00"},
            {"id": 3, "updated_at": "2024-01-01T00:03:00+00:00"},
        ]
        result = compute_next_watermark(
            records, "updated_at", "datetime", fallback="fallback"
        )
        assert result == "2024-01-01T00:05:00+00:00"

    def test_empty_records_returns_fallback(self):
        assert (
            compute_next_watermark([], "updated_at", "datetime", fallback="fallback")
            == "fallback"
        )

    def test_records_missing_column_returns_fallback(self):
        records = [{"id": 1}, {"id": 2}]
        assert (
            compute_next_watermark(
                records, "updated_at", "datetime", fallback="fallback"
            )
            == "fallback"
        )

    def test_computes_max_integer_value(self):
        records = [{"seq": 5}, {"seq": 9}, {"seq": 2}]
        result = compute_next_watermark(records, "seq", "integer", fallback=0)
        assert result == "9"


class TestIncrementalStateStore:
    """Tests for the ingestion_watermark / ingestion_run_history control tables."""

    @pytest.fixture
    def store(self, tmp_path):
        db_path = tmp_path / "test_incremental.db"
        return IncrementalStateStore(f"sqlite:///{db_path}")

    def test_first_run_uses_default_watermark(self, store):
        assert store.get_watermark("orders_api", default="1970-01-01") == "1970-01-01"

    def test_successful_run_advances_watermark(self, store):
        run_id = store.start_run(
            "orders_api", watermark_start="1970-01-01", params_used=json.dumps({})
        )
        store.complete_run(
            run_id,
            source_name="orders_api",
            watermark_end="2024-01-01",
            records_fetched=10,
        )

        assert store.get_watermark("orders_api", default="1970-01-01") == "2024-01-01"

    def test_failed_run_does_not_advance_watermark(self, store):
        run_id = store.start_run(
            "orders_api", watermark_start="1970-01-01", params_used=json.dumps({})
        )
        store.fail_run(run_id, error_message="connection timed out")

        assert store.get_watermark("orders_api", default="1970-01-01") == "1970-01-01"

    def test_run_history_records_both_success_and_failure(self, store):
        run_1 = store.start_run(
            "orders_api", watermark_start="1970-01-01", params_used="{}"
        )
        store.complete_run(
            run_1,
            source_name="orders_api",
            watermark_end="2024-01-01",
            records_fetched=5,
        )

        run_2 = store.start_run(
            "orders_api", watermark_start="2024-01-01", params_used="{}"
        )
        store.fail_run(run_2, error_message="boom")

        history = store.get_history("orders_api")
        statuses = {run.run_id: run.status for run in history}
        assert statuses[run_1] == "success"
        assert statuses[run_2] == "failed"

    def test_sources_are_tracked_independently(self, store):
        run_a = store.start_run("source_a", watermark_start="0", params_used="{}")
        store.complete_run(
            run_a, source_name="source_a", watermark_end="100", records_fetched=1
        )

        assert store.get_watermark("source_a", default="0") == "100"
        assert store.get_watermark("source_b", default="0") == "0"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
