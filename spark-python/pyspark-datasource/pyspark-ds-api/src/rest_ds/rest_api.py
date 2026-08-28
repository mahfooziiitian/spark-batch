import base64
import datetime
import json
import re
import uuid

import jwt
import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from requests import Response
from requests.auth import HTTPBasicAuth


class APIClient:
    def __init__(self, url, opts):
        self.url = url
        self.opts = opts
        self.max_attempts = opts.get("retries", {}).get("maxAttempts", 1)
        self.response_format = opts.get("responseFormat", "json")
        (
            self.headers,
            self.auth,
            self.json_body,
            self.form_body,
            self.query_params,
            self.cert,
        ) = self._build_components()

    def _build_components(self):
        auth_cfg = self.opts.get("authentication", {})
        headers, auth = self._get_auth_headers(auth_cfg)
        json_body, form_body = self._build_body(self.opts.get("body", {}))
        headers.update(self.opts.get("headers", {}))
        query_params = self.opts.get("queryParams", {}).copy()
        if auth_cfg.get("type") == "apikey" and auth_cfg.get("in") == "query":
            query_params[auth_cfg["api_key_name"]] = auth_cfg["api_key_value"]

        cert = (
            (auth_cfg["certFile"], auth_cfg["keyFile"])
            if auth_cfg.get("type") == "mtls"
            else None
        )
        print(f"cert: {cert}")
        return headers, auth, json_body, form_body, query_params, cert

    def _generate_assertion(self, auth_config) -> str:
        with open(auth_config["public_key_path"], "rb") as pub_file:
            public_key = pub_file.read()
        with open(auth_config["private_key_path"], "rb") as priv_file:
            private_key = priv_file.read()

        cert = x509.load_pem_x509_certificate(public_key)
        # x5t/kid thumbprint per RFC 7523 — not used as a security hash.
        fingerprint = cert.fingerprint(hashes.SHA1())  # nosec B303
        x5t = base64.urlsafe_b64encode(fingerprint).decode("utf-8")
        kid = fingerprint.hex()
        request_branch_identifier_key = auth_config.get(
            "request_branch_identifier_key", None
        )
        request_branch_identifier_value = auth_config.get(
            "request_branch_identifier_value", None
        )

        payload = {
            "jti": str(uuid.uuid4()),
            "iat": datetime.datetime.utcnow(),
            "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=5),
            "aud": auth_config["aud"],
            request_branch_identifier_key: request_branch_identifier_value,
        }

        headers = {"x5t": x5t, "kid": kid}
        return jwt.encode(payload, private_key, algorithm="RS256", headers=headers)

    def _generate_bearer_token(self, auth):
        assertion = self._generate_assertion(auth)
        grant_type_key = auth.get("grant_type_key", None)
        grant_type_value = auth.get("grant_type_value", None)
        token_url = auth.get("tokenUrl", None)
        # timeout is always set via auth.get("timeout", 60) below.
        response = requests.post(  # nosec B113
            token_url,
            json={grant_type_key: grant_type_value, "assertion": assertion},
            headers=auth.get("headers", {}),
            timeout=auth.get("timeout", 60),
        )
        response.raise_for_status()
        return response.json().get("access_token")

    def _basic_auth(self, auth):
        if not auth or auth.get("type") != "basic":
            return None

        username = auth.get("username")
        password = auth.get("password")
        if not username or not password:
            raise ValueError("Basic auth requires both username and password.")

        return HTTPBasicAuth(username, password)

    def _get_auth_headers(self, auth):
        if not auth or auth.get("type") == "none":
            return {}, None

        if auth["type"] == "basic":
            return {}, HTTPBasicAuth(auth["username"], auth["password"])

        if auth["type"] == "bearer":
            return {"Authorization": f"Bearer {auth['token']}"}, None

        if auth["type"] == "apikey" and auth["in"] == "header":
            return {auth["api_key_name"]: auth["api_key_value"]}, None

        if auth["type"] == "oauth2_assertion":
            token = self._generate_bearer_token(auth)
            return {"Authorization": f"Bearer {token}"}, None

        if auth["type"] == "oauth2_client_credentials_json":
            client_id_key = auth.get("client_id_key", None)
            client_secret_key = auth.get("client_secret_key", None)
            grant_type_key = auth.get("grant_type_key", None)
            client_id_value = auth.get("client_id_value", None)
            client_secret_value = auth.get("client_secret_value", None)
            grant_type_value = auth.get("grant_type_value", None)
            scope_key = auth.get("scope_key", None)
            scope_value = auth.get("scope_value", None)

            json_data = {
                grant_type_key: grant_type_value,
                client_id_key: client_id_value,
                client_secret_key: client_secret_value,
            }
            if scope_key and scope_value:
                json_data[scope_key] = scope_value
            print(f"json_data: {json_data}")
            resp = requests.post(
                auth["tokenUrl"],
                headers=auth.get("headers", {}),
                json=json_data,
                timeout=60,
            )
            resp.raise_for_status()
            return {"Authorization": f"Bearer {resp.json()['access_token']}"}, None

        if auth["type"] == "oauth2_client_credentials_form":
            client_id_key = auth.get("client_id_key", None)
            client_secret_key = auth.get("client_secret_key", None)
            client_id_value = auth.get("client_id_value", None)
            client_secret_value = auth.get("client_secret_value", None)
            grant_type_key = auth.get("grant_type_key", None)
            grant_type_value = auth.get("grant_type_value", None)
            scope_key = auth.get("scope_key", None)
            scope_value = auth.get("scope_value", None)
            data = {
                client_id_key: client_id_value,
                client_secret_key: client_secret_value,
                grant_type_key: grant_type_value,
            }
            if scope_key and scope_value:
                data[scope_key] = scope_value
            resp = requests.post(
                auth["tokenUrl"],
                headers=auth.get("headers", {}),
                # auth=HTTPBasicAuth(client_id_value, client_secret_value),
                data=data,
                timeout=60,
            )
            resp.raise_for_status()
            return {"Authorization": f"Bearer {resp.json()['access_token']}"}, None

        if auth["type"] == "oauth2_client_credentials_basic":
            client_id_value = auth.get("client_id_value", None)
            client_secret_value = auth.get("client_secret_value", None)
            grant_type_key = auth.get("grant_type_key", None)
            grant_type_value = auth.get("grant_type_value", None)
            scope_key = auth.get("scope_key", None)
            scope_value = auth.get("scope_value", None)
            data = {grant_type_key: grant_type_value}
            if scope_key and scope_value:
                data[scope_key] = scope_value
            resp = requests.post(
                auth["tokenUrl"],
                headers=auth.get("headers", {}),
                auth=HTTPBasicAuth(client_id_value, client_secret_value),
                data=data,
                timeout=60,
            )
            resp.raise_for_status()
            return {"Authorization": f"Bearer {resp.json()['access_token']}"}, None

        if auth["type"] == "oauth2_password_form":
            username_key = auth.get("username_key", None)
            password_key = auth.get("password_key", None)
            username_value = auth.get("username_value", None)
            password_value = auth.get("password_value", None)
            grant_type_key = auth.get("grant_type_key", None)
            grant_type_value = auth.get("grant_type_value", None)

            data = {
                username_key: username_value,
                password_key: password_value,
                grant_type_key: grant_type_value,
            }

            response = requests.post(
                auth["tokenUrl"],
                headers=auth.get("headers", {}),
                data=data,
                timeout=60,
            )

            if response.status_code != 200:
                raise Exception(
                    f"Token request failed: {response.status_code} - {response.text}"
                )

            token = response.json()["access_token"]
            response.raise_for_status()
            return {"Authorization": f"Bearer {token}"}, None

        if auth["type"] == "oauth2_password_json":
            username_key = auth.get("username_key", None)
            password_key = auth.get("password_key", None)
            username_value = auth.get("username_value", None)
            password_value = auth.get("password_value", None)
            grant_type_key = auth.get("grant_type_key", None)
            grant_type_value = auth.get("grant_type_value", None)

            data = {
                username_key: username_value,
                password_key: password_value,
                # grant_type_key: grant_type_value
            }

            response = requests.post(
                auth["tokenUrl"],
                headers=auth.get("headers", {}),
                json=data,
                timeout=60,
            )

            if response.status_code != 200:
                raise Exception(
                    f"Token request failed: {response.status_code} - {response.text}"
                )

            token = response.json()["access_token"]
            response.raise_for_status()
            return {"Authorization": f"Bearer {token}"}, None

        return {}, None

    def _build_body(self, body_cfg):
        if not body_cfg:
            return None, None
        body_type = body_cfg.get("type", "json")
        content = body_cfg.get("content", {})
        if body_type == "json":
            return content, None
        if body_type == "form":
            return None, content
        if body_type == "raw":
            return body_cfg.get("content"), None
        return None, None

    def make_request(self, extra_params=None) -> Response:
        print(f"Going for client request for url: {self.url}")
        if self.response_format not in ("json", "text", "xml", "csv"):
            raise ValueError(f"Unsupported response format: {self.response_format}")
        params = self.query_params.copy()
        if extra_params:
            params.update(extra_params)

        for attempt in range(self.max_attempts):
            print(f"Going for {attempt} attempts")
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
                print("Got response")
                return response
            except Exception as e:
                print(f"Exception: {e}")
                if attempt == self.max_attempts - 1:
                    raise


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

        print(
            f"limit_key: {limit_key}, offset_key: {offset_key}, result_key: {result_key}"
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
            print(f"data: {data}")
            if not data:
                break
            all_data.extend(data)
            offset += limit
            pages_fetched += 1
            print(f"Fetched {len(data)} records, total fetched: {len(all_data)}")
            print(f"Current offset: {offset}, pages fetched: {pages_fetched}")
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

        print(
            f"limit_key: {limit_key}, page_token_key: {page_token_key}, result_key: {result_key}"
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
        print(
            f"page_number_key: {page_number_key}, page_size_key: {page_size_key}, result_key: {result_key}, metadata_prefix: {metadata_prefix}"
        )
        while True:
            params.update({page_number_key: page, page_size_key: page_size_value})
            print(f"params: {params}")
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

            print(
                f"res_total_pages_key: {res_total_pages_key}, res_page_size_key: {res_page_size_key}, res_total_items_key: {res_total_items_key}, res_has_next_key: {res_has_next_key}"
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
            print("has_next_type:", type(has_next))
            print(
                f"page: {page}, total_page_value: {total_page_value}, total_items_value: {total_items_value}, has_next: {has_next}"
            )
            if not data:
                break
            # all_data.extend(data)
            all_data += data if isinstance(data, list) else [data]
            print()
            # Termination steps
            # current_page >= total_pages
            # fetched_items >= total_items
            # has_next == False
            if (
                (total_page_value and page >= total_page_value)
                or (total_items_value and len(all_data) >= total_items_value)
                or (res_has_next_key and not has_next)
            ):
                print(f"inside break after page: {page}")
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

        print(
            f"limit_key: {limit_key}, cursor_key: {cursor_key}, next_cursor_key: {next_cursor_key}, result_key: {result_key}"
        )

        while True:
            params.update({cursor_key: cursor, limit_key: limit})
            # print(f"params: {params}")
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
        with open(filepath, "w") as f:
            for record in data:
                f.write(f"{json.dumps(record, separators=(',', ':'))}\n")
        print(f"JSON response written to {filepath}")

    @staticmethod
    def write_text_response_to_file(response: Response, filepath="response.txt"):
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"Text response written to {filepath}")

    @staticmethod
    def write_binary_response_to_file(response: Response, filepath="file_output.bin"):
        with open(filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Binary content written to {filepath}")

    @staticmethod
    def append_json_pages_to_file(pages, filepath="paginated.jsonl"):
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

    print(f"Extracted {len(all_data)} records.")

    if response_format == "json":
        filepath = opts.get("filepath", "api_response.json")
        if not filepath.endswith(".json"):
            filepath += ".json"
        FileWriter.write_json_response_to_file(all_data, filepath=filepath)
    elif response_format in ("text", "xml", "csv"):
        # For text response, we would need to change implementation to get raw response
        pass
