from unittest.mock import patch

import pytest

from rest_ds.incremental.incremental_runner import (
    IncrementalConfigError,
    run_incremental_ingestion,
)
from rest_ds.incremental.state_store import IncrementalStateStore

BASE_CONFIG = {
    "extracts": {
        "extract": {
            "source": {
                "params": {
                    "location": "http://localhost:9999/events",
                    "options": {
                        "method": "GET",
                        "responseFormat": "json",
                        "incremental": {
                            "enabled": True,
                            "mode": "query_param",
                            "paramName": "updated_since",
                            "watermarkColumn": "updated_at",
                            "type": "datetime",
                            "format": None,
                            "initialValue": "1970-01-01T00:00:00+00:00",
                            "lookback": None,
                            "stateStore": {"url": "sqlite:///:memory:"},
                        },
                    },
                }
            }
        }
    }
}


@pytest.fixture
def state_store(tmp_path):
    return IncrementalStateStore(f"sqlite:///{tmp_path / 'runner_test.db'}")


class TestIncrementalConfigValidation:
    def test_missing_incremental_block_raises(self, spark, state_store):
        config = {
            "extracts": {
                "extract": {
                    "source": {"params": {"location": "http://x", "options": {}}}
                }
            }
        }
        with pytest.raises(IncrementalConfigError):
            run_incremental_ingestion(
                spark, config, "missing_source", state_store=state_store
            )


class TestIncrementalRunner:
    """Tests that the runner threads the watermark through fetch_records and
    updates the control tables correctly, without hitting a real API."""

    def test_first_run_uses_initial_value_and_advances_watermark(
        self, spark, state_store
    ):
        records = [
            {"id": 1, "updated_at": "2024-01-01T00:01:00+00:00"},
            {"id": 2, "updated_at": "2024-01-01T00:02:00+00:00"},
        ]
        with patch(
            "rest_ds.incremental.incremental_runner.fetch_records",
            return_value=(records, {}),
        ) as mock_fetch:
            df = run_incremental_ingestion(
                spark, BASE_CONFIG, "events_api", state_store=state_store
            )

        assert df.count() == 2
        called_extra_params = mock_fetch.call_args.kwargs["extra_query_params"]
        assert called_extra_params == {"updated_since": "1970-01-01T00:00:00+00:00"}
        assert (
            state_store.get_watermark("events_api", default="")
            == "2024-01-01T00:02:00+00:00"
        )

    def test_second_run_uses_previous_watermark(self, spark, state_store):
        first_batch = [{"id": 1, "updated_at": "2024-01-01T00:02:00+00:00"}]
        with patch(
            "rest_ds.incremental.incremental_runner.fetch_records",
            return_value=(first_batch, {}),
        ):
            run_incremental_ingestion(
                spark, BASE_CONFIG, "events_api", state_store=state_store
            )

        with patch(
            "rest_ds.incremental.incremental_runner.fetch_records",
            return_value=([], {}),
        ) as mock_fetch:
            df = run_incremental_ingestion(
                spark, BASE_CONFIG, "events_api", state_store=state_store
            )

        called_extra_params = mock_fetch.call_args.kwargs["extra_query_params"]
        assert called_extra_params == {"updated_since": "2024-01-01T00:02:00+00:00"}
        assert df.count() == 0
        # Empty run must not reset the watermark backward.
        assert (
            state_store.get_watermark("events_api", default="")
            == "2024-01-01T00:02:00+00:00"
        )

    def test_failed_run_records_history_without_advancing_watermark(
        self, spark, state_store
    ):
        with patch(
            "rest_ds.incremental.incremental_runner.fetch_records",
            side_effect=RuntimeError("api unavailable"),
        ):
            with pytest.raises(RuntimeError):
                run_incremental_ingestion(
                    spark, BASE_CONFIG, "events_api", state_store=state_store
                )

        assert state_store.get_watermark("events_api", default="unset") == "unset"
        history = state_store.get_history("events_api")
        assert history[0].status == "failed"
        assert "api unavailable" in history[0].error_message


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
