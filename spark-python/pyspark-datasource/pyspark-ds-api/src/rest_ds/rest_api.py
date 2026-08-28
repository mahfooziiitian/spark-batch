import json
import logging
import os
import re
import time

import requests
from requests import Response

from rest_ds.util.request_builder import build_request_components
from rest_ds.util.response_type import (
    RAW_TEXT_RESPONSE_FORMATS,
    SUPPORTED_RESPONSE_FORMATS,
)

logger = logging.getLogger(__name__)


def _ensure_parent_dir(filepath: str) -> None:
    """Create the parent directory of a local output path if needed.

    No-op for remote destinations (e.g. ``s3://...``) or paths without a
    parent directory component.
    """
    if "://" in filepath:
        return
    parent = os.path.dirname(filepath)
    if parent:
        os.makedirs(parent, exist_ok=True)


class APIClient:
    def __init__(self, url, opts):
        self.url = url
        self.opts = opts
        retries_cfg = opts.get("retries", {})
        self.max_attempts = retries_cfg.get("maxAttempts", 1)
        # Optional exponential backoff between retry attempts: delay is
        # `backoffSeconds * 2**attempt`. Defaults to 0 (no delay) so existing
        # configs keep their current, immediate-retry behavior unless they
        # opt in via `retries.backoffSeconds`.
        self.backoff_seconds = retries_cfg.get("backoffSeconds", 0)
        self.response_format = opts.get("responseFormat", "json")
        # Auth-header dispatch, request-body building and query-param/cert
        # setup are shared with the function-based ingestion path (used by
        # the incremental runner) via `build_request_components` — see
        # `rest_ds.util.request_builder` and `rest_ds.authentication.auth_util`.
        (
            self.headers,
            self.auth,
            self.json_body,
            self.form_body,
            self.query_params,
            self.cert,
        ) = build_request_components(self.opts)

    def make_request(self, extra_params=None) -> Response:
        logger.debug("Going for client request for url: %s", self.url)
        if self.response_format not in SUPPORTED_RESPONSE_FORMATS:
            raise ValueError(f"Unsupported response format: {self.response_format}")
        params = self.query_params.copy()
        if extra_params:
            params.update(extra_params)

        for attempt in range(self.max_attempts):
            logger.debug("Attempt %d of %d", attempt + 1, self.max_attempts)
            try:
                response = requests.request(
                    method=self.opts.get("method", "GET"),
                    url=self.url,
                    headers=self.headers,
                    params=params,
                    json=self.json_body,
                    data=self.form_body,
                    auth=self.auth,
                    cert=self.cert,
                    verify=self.opts.get("authentication", {}).get("caFile", True),
                    timeout=60,
                )
                response.raise_for_status()
                logger.debug("Got response")
                return response
            except requests.RequestException as e:
                logger.warning("Request failed (attempt %d): %s", attempt + 1, e)
                if attempt == self.max_attempts - 1:
                    raise
                if self.backoff_seconds:
                    time.sleep(self.backoff_seconds * (2**attempt))


class Paginator:
    def __init__(self, client: APIClient, **kwargs):
        self.client = client
        self.kwargs = kwargs

    def paginate(self):
        raise NotImplementedError("Subclasses should implement this method.")


class OffsetPaginator(Paginator):
    def paginate(self):
        all_data = []
        offset = 0
        limit = self.kwargs.get("limit", 100)
        max_pages_value = self.kwargs.get("max_pages_value", None)
        pages_fetched = 0
        params = self.client.query_params.copy()
        limit_key = self.kwargs.get("limit_key")
        offset_key = self.kwargs.get("offset_key")
        result_key = self.kwargs.get("result_key")

        logger.debug(
            "limit_key: %s, offset_key: %s, result_key: %s",
            limit_key,
            offset_key,
            result_key,
        )

        while True:
            params.update({limit_key: limit, offset_key: offset})
            response = self.client.make_request(extra_params=params)
            response.raise_for_status()
            response_json = response.json()
            data = (
                read_key_value(response_json, result_key)
                if result_key
                else response_json
            )
            logger.debug("data: %s", data)
            if not data:
                break
            all_data.extend(data)
            offset += limit
            pages_fetched += 1
            logger.debug(
                "Fetched %d records, total fetched: %d", len(data), len(all_data)
            )
            logger.debug("Current offset: %d, pages fetched: %d", offset, pages_fetched)
            if max_pages_value and pages_fetched >= max_pages_value:
                break

        return all_data


class OffsetPageTokenPaginator(Paginator):
    def paginate(self):
        all_data = []
        offset = 0
        limit = self.kwargs.get("limit", 100)
        # max_pages = self.kwargs.get("max_pages")
        pages_fetched = 0
        # url = self.client.url
        # headers = self.client.headers
        params = self.client.query_params.copy()
        # timeout = self.kwargs.get("timeout", 60)
        limit_key = self.kwargs.get("limit_key")
        page_token_key = self.kwargs.get("page_token_key")
        next_page_token_key = self.kwargs.get("next_page_token_key")
        result_key = self.kwargs.get("result_key")
        page_token_value = None

        logger.debug(
            "limit_key: %s, page_token_key: %s, result_key: %s",
            limit_key,
            page_token_key,
            result_key,
        )

        while True:
            params.update({limit_key: limit, page_token_key: page_token_value})
            # response = requests.get(url, headers=headers, params=params, timeout=timeout)
            response = self.client.make_request(extra_params=params)
            response.raise_for_status()
            json_response = response.json()
            data = (
                read_key_value(json_response, result_key)
                if result_key
                else json_response
            )
            if not data:
                break
            all_data.extend(data)
            offset += limit
            pages_fetched += 1
            page_token_value = (
                json_response[next_page_token_key] if next_page_token_key else None
            )
            if page_token_value is None:
                break

        return all_data


class PageNumberPaginator(Paginator):
    def paginate(self):
        all_data = []
        page_number_key = self.kwargs.get("page_number_key", "page")
        page_size_key = self.kwargs.get("page_size_key", "pageSize")
        page_size_value = self.kwargs.get("page_size_value", 50)
        total_pages_key = self.kwargs.get("total_pages_key", None)
        total_page_value = self.kwargs.get("total_page_value", None)
        total_items_key = self.kwargs.get("total_items_key", None)
        has_next_key = self.kwargs.get("has_next_key", False)
        metadata_prefix = self.kwargs.get("metadata_prefix", "")
        page = self.kwargs.get("start_page", 1)
        result_key = self.kwargs.get("result_key", None)
        params = self.client.query_params.copy()
        logger.debug(
            "page_number_key: %s, page_size_key: %s, result_key: %s, metadata_prefix: %s",
            page_number_key,
            page_size_key,
            result_key,
            metadata_prefix,
        )
        while True:
            params.update({page_number_key: page, page_size_key: page_size_value})
            logger.debug("params: %s", params)
            # response = requests.get(url, headers=headers, params=params, timeout=timeout)
            response = self.client.make_request(extra_params=params)
            response.raise_for_status()
            json_response = response.json()
            # metadata_prefix = metadata_prefix + "." if metadata_prefix else ""
            if metadata_prefix == "":
                res_total_pages_key = total_pages_key
                res_page_size_key = page_size_key
                res_has_next_key = has_next_key
                res_total_items_key = total_items_key
            else:
                res_total_pages_key = (
                    f"{metadata_prefix}.{total_pages_key}" if total_pages_key else None
                )
                res_page_size_key = (
                    f"{metadata_prefix}.{page_size_key}" if page_size_key else None
                )
                res_total_items_key = (
                    f"{metadata_prefix}.{total_items_key}" if total_items_key else None
                )
                res_has_next_key = (
                    f"{metadata_prefix}.{has_next_key}" if has_next_key else None
                )

            logger.debug(
                "res_total_pages_key: %s, res_page_size_key: %s, "
                "res_total_items_key: %s, res_has_next_key: %s",
                res_total_pages_key,
                res_page_size_key,
                res_total_items_key,
                res_has_next_key,
            )

            total_page_value = (
                (read_key_value(json_response, res_total_pages_key) or total_page_value)
                if res_total_pages_key
                else None
            )
            data = (
                read_key_value(json_response, result_key)
                if result_key
                else json_response
            )
            total_items_value = (
                read_key_value(json_response, res_total_items_key)
                if res_total_items_key
                else None
            )
            has_next = (
                read_key_value(json_response, res_has_next_key)
                if res_has_next_key
                else False
            )
            logger.debug("has_next_type: %s", type(has_next))
            logger.debug(
                "page: %s, total_page_value: %s, total_items_value: %s, has_next: %s",
                page,
                total_page_value,
                total_items_value,
                has_next,
            )
            if not data:
                break
            # all_data.extend(data)
            all_data += data if isinstance(data, list) else [data]
            # Termination steps
            # current_page >= total_pages
            # fetched_items >= total_items
            # has_next == False
            if (
                (total_page_value and page >= total_page_value)
                or (total_items_value and len(all_data) >= total_items_value)
                or (res_has_next_key and not has_next)
            ):
                logger.debug("Breaking pagination after page: %s", page)
                break
            page += 1

        return all_data


class CursorPaginator(Paginator):
    def paginate(self):
        all_data = []
        limit_key = self.kwargs.get("limit_key", None)
        cursor_key = self.kwargs.get("cursor_key", None)
        next_cursor_key = self.kwargs.get("next_cursor_key", None)
        limit = self.kwargs.get("limit", 100)
        result_key = self.kwargs.get("result_key", None)
        params = self.client.query_params.copy()
        cursor = None

        logger.debug(
            "limit_key: %s, cursor_key: %s, next_cursor_key: %s, result_key: %s",
            limit_key,
            cursor_key,
            next_cursor_key,
            result_key,
        )

        while True:
            params.update({cursor_key: cursor, limit_key: limit})
            # response = requests.get(url, headers=headers, params=params, timeout=timeout)
            response = self.client.make_request(extra_params=params)
            response.raise_for_status()
            json_reponse = response.json()

            cursor = (
                read_key_value(json_reponse, next_cursor_key)
                if next_cursor_key
                else None
            )

            records = (
                read_key_value(json_reponse, result_key) if result_key else json_reponse
            )
            if not records:
                break

            all_data.extend(records)
            if not cursor:
                break

        return all_data


class LinkHeaderPaginator(Paginator):
    @staticmethod
    def extract_next_link(link_header):
        matches = re.findall(r'<([^>]+)>;\s*rel="next"', link_header)
        return matches[0] if matches else None

    def paginate(self):
        all_data = []
        url = self.client.url
        headers = self.client.headers
        params = self.client.query_params.copy()
        timeout = self.kwargs.get("timeout", 60)

        while url:
            response = requests.get(
                url, headers=headers, params=params, timeout=timeout
            )
            response.raise_for_status()
            data = response.json()
            all_data.extend(data)

            link_header = response.headers.get("Link")
            url = self.extract_next_link(link_header) if link_header else None
            params = {}  # reset for subsequent full URLs

        return all_data


class PaginationFactory:
    @staticmethod
    def get_paginator(client, strategy, **kwargs):
        if strategy == "offset":
            return OffsetPaginator(client, **kwargs)
        elif strategy == "page":
            return PageNumberPaginator(client, **kwargs)
        elif strategy == "cursor":
            return CursorPaginator(client, **kwargs)
        elif strategy == "link":
            return LinkHeaderPaginator(client, **kwargs)
        elif strategy == "offset_page_token":
            return OffsetPageTokenPaginator(client, **kwargs)
        else:
            raise ValueError("Unsupported pagination strategy.")


class FileWriter:
    @staticmethod
    def write_json_response_to_file(data, filepath="response.json"):
        _ensure_parent_dir(filepath)
        with open(filepath, "w") as f:
            for record in data:
                f.write(f"{json.dumps(record, separators=(',', ':'))}\n")
        logger.info("JSON response written to %s", filepath)

    @staticmethod
    def write_text_response_to_file(response: Response, filepath="response.txt"):
        _ensure_parent_dir(filepath)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(response.text)
        logger.info("Text response written to %s", filepath)

    @staticmethod
    def write_binary_response_to_file(response: Response, filepath="file_output.bin"):
        _ensure_parent_dir(filepath)
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info("Binary content written to %s", filepath)

    @staticmethod
    def append_json_pages_to_file(pages, filepath="paginated.jsonl"):
        _ensure_parent_dir(filepath)
        with open(filepath, "w", encoding="utf-8") as f:
            for page in pages:
                if isinstance(page, list):
                    for item in page:
                        f.write(json.dumps(item) + "\n")
                else:
                    f.write(json.dumps(page) + "\n")


def read_key_value(json_dict: dict, key: str):
    keys = key.split(".")
    result = json_dict
    for k in keys:
        if result.get(k, None) is None:
            return None
        else:
            result = result.get(k, None)
    return result


def read_api(spark, config):
    src = config["extracts"]["extract"]["source"]["params"]
    opts = src["options"]
    pagination_cfg = opts.get("pagination", {})
    response_format = opts.get("responseFormat", "json")
    url = src["location"]

    client = APIClient(url, opts)

    if pagination_cfg:
        paginator = PaginationFactory.get_paginator(client, **pagination_cfg)
        all_data = paginator.paginate()
    else:
        result = client.make_request()
        json_response = result.json()
        if opts.get("result_key"):
            json_response = read_key_value(json_response, opts["result_key"])
        all_data = json_response if isinstance(json_response, list) else [json_response]

    logger.info("Extracted %d records.", len(all_data))

    if response_format == "json":
        filepath = opts.get("filepath", "api_response.json")
        if not filepath.endswith(".json"):
            filepath += ".json"
        FileWriter.write_json_response_to_file(all_data, filepath=filepath)
    elif response_format in RAW_TEXT_RESPONSE_FORMATS:
        # For text response, we would need to change implementation to get raw response
        pass
