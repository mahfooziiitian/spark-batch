import re
import requests


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
            elif responseFormat in ("text", "xml", "csv"):
                return {} if response.status_code == 204 else response.text
            else:
                raise Exception(f"Unsupported response format: {responseFormat}")
        except Exception as e:
            if attempt == max_attempts - 1:
                raise e


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


def offset_pagination(url, headers, params, limit=100, timeout=60, max_pages=None):
    all_data = []
    offset = 0
    pages_fetched = 0

    while True:
        params.update({"limit": limit, "offset": offset})
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        if not data:
            break
        all_data.extend(data)

        offset += limit
        pages_fetched += 1
        if max_pages and pages_fetched >= max_pages:
            break

    return all_data


def page_number_pagination(
    url, headers, params, page_size=50, timeout=60, max_pages=None
):
    all_data = []
    page = 1

    while True:
        params.update({"page": page, "pageSize": page_size})
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        if not data:
            break
        all_data.extend(data)

        if max_pages and page >= max_pages:
            break

        page += 1

    return all_data


def cursor_based_pagination(
    url, headers, params, cursor_param="cursor", next_key="next", timeout=60
):
    all_data = []
    cursor = None

    while True:
        if cursor:
            params[cursor_param] = cursor
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        records = data.get("results") or data.get("data") or data
        all_data.extend(records)

        cursor = data.get(next_key)
        if not cursor:
            break

    return all_data


def extract_next_link(link_header):
    matches = re.findall(r'<([^>]+)>;\s*rel="next"', link_header)
    return matches[0] if matches else None


def link_header_pagination(url, headers, params, timeout=60):
    all_data = []

    while url:
        response = requests.get(url, headers=headers, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        all_data.extend(data)

        link_header = response.headers.get("Link")
        url = extract_next_link(link_header) if link_header else None
        params = {}  # reset for subsequent full URLs

    return all_data


def fetch_paginated_data(strategy, **kwargs):
    if strategy == "offset":
        return offset_pagination(**kwargs)
    elif strategy == "page":
        return page_number_pagination(**kwargs)
    elif strategy == "cursor":
        return cursor_based_pagination(**kwargs)
    elif strategy == "link":
        return link_header_pagination(**kwargs)
    else:
        raise ValueError("Unsupported pagination strategy.")
