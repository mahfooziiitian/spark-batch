from pyspark.sql.types import StructType

from rest_ds.schema.json_schema import read_json_schema
from rest_ds.util.api_client import fetch_data_with_pagination, make_request
from rest_ds.util.request_builder import build_request_components


def create_dataframe_json(spark, data, schema_path=None):
    schema = None
    if schema_path and schema_path.endswith(".json"):
        schema = read_json_schema(schema_path)
    if not data:
        # Spark cannot infer a schema from an empty list. This matters most
        # for incremental runs where "nothing new since the last watermark"
        # is a normal, expected outcome rather than an error — fall back to
        # the declared schema when available, or an empty StructType so the
        # caller still gets a valid (zero-row) DataFrame instead of a crash.
        return spark.createDataFrame([], schema=schema or StructType([]))
    if schema:
        return spark.createDataFrame(data, schema=schema)
    return spark.createDataFrame(data)


def fetch_records(config, extra_query_params=None):
    """Fetch the raw record list for a YAML-configured source, without
    building a Spark DataFrame. This is shared by `read_api()` (one-shot
    ingestion) and the incremental runner (`incremental/incremental_runner.py`),
    which needs the raw records to compute the next watermark before handing
    them to Spark.

    `extra_query_params` are merged into the base query params for every
    request in the run (e.g. an incremental watermark parameter); pagination
    parameters are still layered on top per-page as before.
    """
    src = config["extracts"]["extract"]["source"]["params"]
    opts = src["options"]
    pagination = opts.get("pagination", {})
    response_format = opts.get("responseFormat", "json")
    max_attempts = opts.get("retries", {}).get("maxAttempts", 1)
    url = src["location"]

    headers, auth, json_body, form_body, query_params, cert = build_request_components(
        opts
    )
    query_params = query_params.copy()
    if extra_query_params:
        query_params.update(extra_query_params)

    def make_request_fn(extra=None):
        return make_request(
            url,
            opts,
            headers,
            auth,
            cert,
            query_params,
            json_body,
            form_body,
            max_attempts,
            response_format,
            extra,
        )

    if pagination:
        all_data = fetch_data_with_pagination(make_request_fn, pagination)
    else:
        result = make_request_fn()
        all_data = result if isinstance(result, list) else [result]

    return all_data, opts


def read_api(spark, config, extra_query_params=None):
    all_data, opts = fetch_records(config, extra_query_params=extra_query_params)
    print(f"Extracted {len(all_data)} records.")
    return create_dataframe_json(spark, all_data, schema_path=opts.get("schema"))
