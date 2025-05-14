import requests

# Paths to your certificate and CA files
CLIENT_CERT = "client_cert.pem"  # Your client certificate (can also be a tuple of cert & key)
CA_BUNDLE = "ca_bundle.pem"  # The CA file to validate the server's certificate

# The API endpoint you're trying to access
url = "https://your-api-server.example.com/secure-endpoint"

try:
    response = requests.get(
        url, cert=CLIENT_CERT, verify=CA_BUNDLE, timeout=30  # Client certificate  # CA bundle to verify server's cert
    )

    response.raise_for_status()
    print("Response Status:", response.status_code)
    print("Response Body:", response.text)

except requests.exceptions.SSLError as ssl_err:
    print("SSL error:", ssl_err)

except requests.exceptions.RequestException as req_err:
    print("Request failed:", req_err)
