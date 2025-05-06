import os
import sys
from pathlib import Path

import requests
import yaml
from pyspark.sql import SparkSession
from requests.auth import HTTPBasicAuth

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_11"]
os.environ["PYSPARK_PYTHON"] = sys.executable


def get_auth_headers(auth):
    if not auth or auth.get("type") == "none":
        return {}, None

    if auth["type"] == "basic":
        print("Basic Authentication")
        return {}, HTTPBasicAuth(auth["username"], auth["password"])

    if auth["type"] == "bearer":
        print("Bearer Authentication")
        return {"Authorization": f"Bearer {auth['token']}"}, None

    if auth["type"] == "apikey":
        print("API Key Authentication")
        if auth["in"] == "header":
            print("API Key in Header")
            return {auth["name"]: auth["value"]}, None
        # if in query, return empty and let caller add to params
        return {}, None

    if auth["type"] == "oauth2_client_credentials":
        print("OAuth2 Client Credentials flow")
        headers = auth.get("headers", {})
        print("Headers: ", headers)
        data = {
            "grant_type": "client_credentials",
            "client_id": auth["clientId"],
            "client_secret": auth["clientSecret"],
            "scope": auth.get("scope", ""),
        }
        print("Data: ", data)
        token_resp = requests.post(
            url=auth["tokenUrl"],
            headers=headers,
            data=data,
        )
        token_resp.raise_for_status()
        token = token_resp.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}, None

    if auth["type"] == "oauth2_form_client_credentials":
        print("OAuth2 Client Credentials flow")
        headers = auth.get("headers", {})
        print("Headers: ", headers)
        data = {"grant_type": "client_credentials"}
        client_id = auth.get("clientId")
        client_secret = auth.get("clientSecret")
        basic_auth = HTTPBasicAuth(client_id, client_secret)
        print("Data: ", data)
        token_resp = requests.post(
            url=auth["tokenUrl"],
            headers=headers,
            auth=basic_auth,
            data=data,
        )
        token_resp.raise_for_status()
        token = token_resp.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}, None

    return {}, None


def read_api(spark, config):
    src = config["extracts"]["extract"]["source"]["params"]
    opts = src["options"]
    auth_cfg = opts.get("authentication", {})
    pagination = opts.get("pagination", {})
    responseFormat = opts.get("responseFormat", "json")

    headers, auth = get_auth_headers(auth_cfg)
    json_body, form_body = build_request_body(opts.get("body", {}))
    headers.update(opts.get("headers", {}))
    queryParams = opts.get("queryParams", {}).copy()

    if auth_cfg.get("type") == "apikey" and auth_cfg.get("in") == "query":
        queryParams[auth_cfg["name"]] = auth_cfg["value"]

    cert = None
    if auth_cfg.get("type") == "mtls":
        cert = (auth_cfg["certFile"], auth_cfg["keyFile"])

    url = src["location"]
    all_data = []
    max_attempts = opts.get("retries", {}).get("maxAttempts", 1)

    def make_request(extra_params=None):
        print(f"Making request to {url} with params: {queryParams}")
        print(f"Headers: {headers}")
        print(f"Body: {json_body or form_body}")
        params = queryParams.copy()
        if extra_params:
            params.update(extra_params)
        for attempt in range(max_attempts):
            try:
                response = requests.request(
                    method=opts.get("method", "GET"),
                    url=url,
                    headers=headers,
                    params=params,
                    json=json_body,
                    data=form_body,
                    auth=auth,
                    cert=cert,
                    verify=auth_cfg.get("caFile", True),
                )
                print(f"Response status code: {response.status_code}")
                print(f"Response content: {response.content}")
                response.raise_for_status()
                if responseFormat == "json":
                    if response.status_code == 204:
                        return {}
                    if response.status_code == 200:
                        return response.json()
                    else:
                        raise Exception(f"Unexpected status code: {response.status_code}")
                elif responseFormat == "text":
                    if response.status_code == 204:
                        return {}
                    if response.status_code == 200:
                        return response.text
                    else:
                        raise Exception(f"Unexpected status code: {response.status_code}")
                elif responseFormat == "xml":
                    if response.status_code == 204:
                        return {}
                    if response.status_code == 200:
                        return response.text
                    else:
                        raise Exception(f"Unexpected status code: {response.status_code}")
                elif responseFormat == "csv":
                    if response.status_code == 204:
                        return {}
                    if response.status_code == 200:
                        return response.text
                    else:
                        raise Exception(f"Unexpected status code: {response.status_code}")
                else:
                    raise Exception(f"Unsupported response format: {responseFormat}")
            except Exception as e:
                if attempt == max_attempts - 1:
                    raise e

    # 🔁 Paginated or Single-shot Request
    if pagination:
        print("Paginated request")
        current_skip = pagination.get("skip", 0)
        limit = pagination.get("limit", 100)
        page_size = pagination.get("pageSize", 100)
        while current_skip < limit:
            response_json = make_request({"skip": current_skip, "limit": page_size})
            extracted = response_json
            all_data.append(extracted)
            current_skip += page_size
    else:
        # 📦 Single non-paginated request
        print("Non paginated request")
        response_json = make_request()
        print(f"Response JSON: {response_json}")
        extracted = response_json
        all_data.append(extracted)
    print(f"Extracted {len(all_data)} records from API.")
    print(f"Response: {all_data}")
    # 🧪 Convert to Spark DataFrame
    return create_dataframe_json(spark, all_data)


def create_dataframe_json(spark: SparkSession, all_data: list):
    return spark.createDataFrame(all_data)


def build_request_body(body_cfg):
    if not body_cfg:
        return None, None

    body_type = body_cfg.get("type", "json")
    content = body_cfg.get("content", {})

    if body_type == "json":
        return content, None
    elif body_type == "form":
        return None, content
    elif body_type == "raw":
        return body_cfg.get("content"), None
    return None, None


def main():
    spark = SparkSession.builder.appName("REST_API_Ingestion").getOrCreate()
    config_path = Path(__file__).parents[0] / "api_key_query.yaml"
    print(f"Loading config from {config_path}")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    print(f"Config loaded: {config}")
    print("Extracting data from API...")
    df = read_api(spark, config)
    print(f"Data extracted: {df.show(truncate=False, n=100)}")
    print("Data extraction complete.")


if __name__ == "__main__":
    main()
