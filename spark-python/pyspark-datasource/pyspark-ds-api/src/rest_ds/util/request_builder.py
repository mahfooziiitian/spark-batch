from rest_ds.authentication.auth_util import get_auth_headers


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


def build_request_components(opts):
    auth_cfg = opts.get("authentication", {})
    headers, auth = get_auth_headers(auth_cfg)
    json_body, form_body = build_request_body(opts.get("body", {}))
    headers.update(opts.get("headers", {}))
    queryParams = opts.get("queryParams", {}).copy()

    if auth_cfg.get("type") == "apikey" and auth_cfg.get("in") == "query":
        queryParams[auth_cfg["name"]] = auth_cfg["value"]

    cert = None
    if auth_cfg.get("type") == "mtls":
        cert = (auth_cfg["certFile"], auth_cfg["keyFile"])

    return headers, auth, json_body, form_body, queryParams, cert
