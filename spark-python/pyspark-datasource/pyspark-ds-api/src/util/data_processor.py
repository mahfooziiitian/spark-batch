from schema.json_schema import read_json_schema
from util.request_builder import build_request_components
from util.api_client import make_request, fetch_data_with_pagination


def create_dataframe_json(spark, data, schema_path=None):
    schema = None
    if schema_path and schema_path.endswith(".json"):
        schema = read_json_schema(schema_path)
    if schema:
        return spark.createDataFrame(data, schema=schema)
    return spark.createDataFrame(data)


def read_api(spark, config):
    src = config["extracts"]["extract"]["source"]["params"]
    opts = src["options"]
    pagination = opts.get("pagination", {})
    response_format = opts.get("responseFormat", "json")
    max_attempts = opts.get("retries", {}).get("maxAttempts", 1)
    url = src["location"]

    headers, auth, json_body, form_body, query_params, cert = build_request_components(
        opts
    )

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

    print(f"Extracted {len(all_data)} records.")
    return create_dataframe_json(spark, all_data, schema_path=opts.get("schema"))
