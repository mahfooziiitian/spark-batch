"""Orchestrates a single incremental REST API ingestion run.

Flow per run:

1. Read the last successful watermark for this source from the
   ``ingestion_watermark`` control table (or the YAML `initialValue` on the
   very first run).
2. Apply an optional lookback/overlap window (for late-arriving records).
3. Inject the watermark as a query/body/header parameter and fetch data,
   reusing the existing `APIClient` + pagination + auth machinery untouched.
4. Compute the new high-watermark from the fetched records.
5. On success: record the run in `ingestion_run_history` and advance
   `ingestion_watermark` in a single transaction.
6. On failure: record the failed run; the watermark is left untouched so the
   next run retries the same window.
"""

import json
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from rest_ds.incremental.state_store import IncrementalStateStore
from rest_ds.incremental.watermark import (apply_lookback,
                                           compute_next_watermark)
from rest_ds.util.data_processor import create_dataframe_json, fetch_records


class IncrementalConfigError(ValueError):
    """Raised when a source's YAML config is missing a valid `incremental` block."""


def _incremental_config(config: dict) -> dict:
    opts = config["extracts"]["extract"]["source"]["params"]["options"]
    inc_cfg = opts.get("incremental")
    if not inc_cfg or not inc_cfg.get("enabled", False):
        raise IncrementalConfigError(
            "Source config has no `options.incremental` block with `enabled: true`. "
            "See examples/incremental/README.md for the expected shape."
        )
    required = ("paramName", "watermarkColumn", "initialValue", "stateStore")
    missing = [key for key in required if key not in inc_cfg]
    if missing:
        raise IncrementalConfigError(
            f"`incremental` config is missing required key(s): {missing}"
        )
    return inc_cfg


def run_incremental_ingestion(
    spark: SparkSession,
    config: dict,
    source_name: str,
    state_store: Optional[IncrementalStateStore] = None,
) -> DataFrame:
    """Run one incremental ingestion cycle for `source_name` and return the
    resulting Spark DataFrame (which may be empty if there was nothing new)."""
    inc_cfg = _incremental_config(config)

    value_type = inc_cfg.get("type", "string")
    date_format = inc_cfg.get("format")
    param_name = inc_cfg["paramName"]
    injection_mode = inc_cfg.get("mode", "query_param")
    watermark_column = inc_cfg["watermarkColumn"]
    lookback = inc_cfg.get("lookback")

    state_store = state_store or IncrementalStateStore(inc_cfg["stateStore"]["url"])

    last_watermark = state_store.get_watermark(
        source_name, default=inc_cfg["initialValue"]
    )
    run_watermark = apply_lookback(last_watermark, lookback, value_type, date_format)

    if injection_mode != "query_param":
        raise NotImplementedError(
            f"incremental.mode={injection_mode!r} is not yet supported; only 'query_param' is implemented."
        )
    extra_params = {param_name: run_watermark}

    run_id = state_store.start_run(
        source_name=source_name,
        watermark_start=last_watermark,
        params_used=json.dumps(extra_params),
    )

    try:
        all_data, source_opts = fetch_records(config, extra_query_params=extra_params)
        new_watermark = compute_next_watermark(
            all_data,
            watermark_column,
            value_type,
            date_format,
            fallback=last_watermark,
        )
        # `last_watermark` is always populated (from `inc_cfg["initialValue"]`
        # on the first run), so the fallback path never actually returns None.
        assert new_watermark is not None
        df = create_dataframe_json(
            spark, all_data, schema_path=source_opts.get("schema")
        )
        state_store.complete_run(
            run_id=run_id,
            source_name=source_name,
            watermark_end=new_watermark,
            records_fetched=len(all_data),
        )
        print(
            f"[incremental] source={source_name} run_id={run_id} status=success "
            f"records={len(all_data)} watermark {last_watermark!r} -> {new_watermark!r}"
        )
        return df
    except Exception as exc:  # noqa: BLE001 - re-raised after recording failure
        state_store.fail_run(run_id=run_id, error_message=str(exc))
        print(
            f"[incremental] source={source_name} run_id={run_id} status=failed error={exc}"
        )
        raise
