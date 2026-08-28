import time
import webbrowser

import requests

client_id = "Ov23liu6RQn7dtNHiP4g"
scope = "repo"

# Step 1: Get device code
device_code_resp = requests.post(
    "https://github.com/login/device/code",
    data={"client_id": client_id, "scope": scope},
    headers={"Accept": "application/json"},
).json()

print(
    "Go to {} and enter the code: {}".format(
        device_code_resp["verification_uri"], device_code_resp["user_code"]
    )
)

webbrowser.open(device_code_resp["verification_uri"])

# Step 2: Poll for token
interval = device_code_resp["interval"]
while True:
    time.sleep(interval)
    token_resp = requests.post(
        "https://github.com/login/oauth/access_token",
        data={
            "client_id": client_id,
            "device_code": device_code_resp["device_code"],
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        },
        headers={"Accept": "application/json"},
    ).json()

    if "access_token" in token_resp:
        print("Access Token:", token_resp["access_token"])
        break
    elif token_resp.get("error") != "authorization_pending":
        print("Error:", token_resp)
        break

# Step 3: Use token to call GitHub API
access_token = token_resp["access_token"]
user_info = requests.get(
    "https://api.github.com/user",
    headers={"Authorization": f"Bearer {access_token}", "User-Agent": "my-cli-tool"},
    verify="/home/malam2/development/learning/spark-batch/spark-python/pyspark-datasource/pyspark-ds-api/src/authentication/oauth2/device_code/zscaler_root_ca.crt",
).json()

print("Authenticated as:", user_info["login"])
