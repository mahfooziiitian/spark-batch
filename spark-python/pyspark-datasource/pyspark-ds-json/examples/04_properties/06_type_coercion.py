"""Type coercion — control how Spark infers JSON value types.

Demonstrates options that affect type inference during JSON schema
discovery, including forcing string types, decimal preference,
sampling ratio, and locale settings.

Key concepts:
    - primitivesAsString: read all primitive values as strings
    - prefersDecimal: infer floating-point as decimal instead of double
    - samplingRatio: fraction of records sampled for schema inference
    - locale: IETF BCP 47 language tag for date/number parsing

Reference:
    https://spark.apache.org/docs/latest/sql-data-sources-json.html
"""

from __future__ import annotations

import json
import os
import tempfile

from pys_json import (
    get_spark,
    print_dataframe,
    print_header,
    print_schema,
    print_success,
    print_warning,
    set_log_level,
    write_json_lines,
)
from pys_json._logging import get_logger

set_log_level("DEBUG")
logger = get_logger("example.type_coercion")


def _output_path(filename: str) -> str:
    """Build an output path for example JSON data."""
    out_dir = os.path.join(tempfile.gettempdir(), "pys_json", "type_coercion")
    return os.path.join(out_dir, filename)


def _write_records(filename: str, records: list[dict[str, object]]) -> str:
    """Write JSON records as newline-delimited input data."""
    path = _output_path(filename)
    lines = [json.dumps(record) for record in records]
    logger.debug("Writing %d records to %s", len(records), path)
    return write_json_lines(path, lines)


if __name__ == "__main__":
    spark = get_spark("type-coercion")

    try:
        print_header("1. primitivesAsString=true")
        primitive_path = _write_records(
            "01_primitives_as_string.json",
            [
                {"id": 1, "active": True, "ratio": 3.14159, "label": "alpha"},
                {"id": 2, "active": False, "ratio": 2.71828, "label": "beta"},
            ],
        )
        logger.info("Reading primitive data from %s", primitive_path)

        primitive_strings_df = spark.read.option("primitivesAsString", "true").json(primitive_path)
        primitive_default_df = spark.read.json(primitive_path)

        print_schema(primitive_strings_df, title="Schema with primitivesAsString=true")
        print_dataframe(primitive_strings_df, title="Data with primitivesAsString=true")
        print_schema(primitive_default_df, title="Default inferred schema")
        print_dataframe(primitive_default_df, title="Data with default inference")
        print_warning("Use primitivesAsString when you need exact string representations of source values.")

        print_header("2. prefersDecimal=true")
        decimal_path = _write_records(
            "02_prefers_decimal.json",
            [
                {"sku": "A100", "price": 19.99, "tax": 0.0825},
                {"sku": "B200", "price": 1250.10, "tax": 0.0950},
            ],
        )
        logger.info("Reading decimal data from %s", decimal_path)

        decimal_preferred_df = spark.read.option("prefersDecimal", "true").json(decimal_path)
        decimal_default_df = spark.read.json(decimal_path)

        print_schema(decimal_preferred_df, title="Schema with prefersDecimal=true")
        print_dataframe(decimal_preferred_df, title="Data with prefersDecimal=true")
        print_schema(decimal_default_df, title="Default inferred schema")
        print_dataframe(decimal_default_df, title="Data with default inference")
        print_success("prefersDecimal is a strong fit for financial data that requires exact precision.")

        print_header("3. samplingRatio")
        sampling_records = [{"id": row_id, "category": "standard"} for row_id in range(1, 11)]
        sampling_records.extend(
            [
                {"id": 11, "category": "extended", "rare_text": "appears only once"},
                {"id": 12, "category": "extended", "rare_flag": True, "rare_metric": 99.9},
            ]
        )
        sampling_path = _write_records("03_sampling_ratio.json", sampling_records)
        logger.info("Reading sampling data from %s", sampling_path)
        logger.info("samplingRatio trade-off: lower values are faster, higher values improve schema accuracy.")

        sampled_schema_df = spark.read.option("samplingRatio", "0.3").json(sampling_path)
        full_schema_df = spark.read.option("samplingRatio", "1.0").json(sampling_path)

        print_schema(sampled_schema_df, title="Schema with samplingRatio=0.3")
        print_schema(full_schema_df, title="Schema with samplingRatio=1.0")
        print_dataframe(sampled_schema_df.orderBy("id"), title="Data with samplingRatio=0.3", max_rows=12)
        print_dataframe(full_schema_df.orderBy("id"), title="Data with samplingRatio=1.0", max_rows=12)
        print_warning("Low sampling ratios may miss fields that appear only in unsampled records.")

        print_header("4. locale")
        locale_path = _write_records(
            "04_locale.json",
            [
                {"event_date": "15-Mar-2024", "label": "quarter checkpoint"},
                {"event_date": "01-Jan-2025", "label": "new year start"},
            ],
        )
        logger.info("Reading locale-aware date data from %s", locale_path)
        logger.info("Locale matters for month names such as %s", "Mar versus Mär versus Mars")

        locale_df = (
            spark.read.schema("event_date DATE, label STRING")
            .option("locale", "en-US")
            .option("dateFormat", "dd-MMM-yyyy")
            .json(locale_path)
        )

        print_schema(locale_df, title="Schema with locale=en-US and dateFormat=dd-MMM-yyyy")
        print_dataframe(locale_df, title="Data parsed with locale-aware dates")
    finally:
        spark.stop()
