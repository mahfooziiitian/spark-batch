import requests
from requests.auth import HTTPBasicAuth


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
            url=auth["tokenUrl"], headers=headers, data=data, timeout=60
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
            timeout=60,
        )
        token_resp.raise_for_status()
        token = token_resp.json()["access_token"]
        return {"Authorization": f"Bearer {token}"}, None

    return {}, None
