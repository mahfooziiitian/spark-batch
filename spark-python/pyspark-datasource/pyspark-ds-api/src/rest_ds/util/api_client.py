import time

import requests

from rest_ds.util.response_type import RAW_TEXT_RESPONSE_FORMATS


def make_request(
    url,
    opts,
    headers,
    auth,
    cert,
    queryParams,
    json_body,
    form_body,
    max_attempts,
    responseFormat,
    extra_params=None,
):
    params = queryParams.copy()
    if extra_params:
        params.update(extra_params)

    # Optional exponential backoff between retry attempts: delay is
    # `backoffSeconds * 2**attempt`. Defaults to 0 (no delay) so existing
    # configs keep their current, immediate-retry behavior unless they opt
    # in via `retries.backoffSeconds`.
    backoff_seconds = opts.get("retries", {}).get("backoffSeconds", 0)

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
                verify=opts.get("authentication", {}).get("caFile", True),
                timeout=60,
            )
            response.raise_for_status()

            if responseFormat == "json":
                return {} if response.status_code == 204 else response.json()
            elif responseFormat in RAW_TEXT_RESPONSE_FORMATS:
                return {} if response.status_code == 204 else response.text
            else:
                raise ValueError(f"Unsupported response format: {responseFormat}")
        except requests.RequestException:
            if attempt == max_attempts - 1:
                raise
            if backoff_seconds:
                time.sleep(backoff_seconds * (2**attempt))


def fetch_data_with_pagination(make_request_fn, pagination):
    all_data = []
    current_skip = pagination.get("skip", 0)
    limit = pagination.get("limit", 100)
    page_size = pagination.get("pageSize", 100)
    while current_skip < limit:
        response_json = make_request_fn({"skip": current_skip, "limit": page_size})
        if isinstance(response_json, list):
            all_data.extend(response_json)
        else:
            all_data.append(response_json)
        current_skip += page_size
    return all_data
