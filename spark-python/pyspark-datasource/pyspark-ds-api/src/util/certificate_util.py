import base64
import datetime
import ssl
import socket
import uuid
import jwt
import requests
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from requests.exceptions import HTTPError


def generate_assertion(public_key_path: str, private_key_path: str) -> str:
    """Generates a JWT assertion for GPCS STS."""
    with open(public_key_path, mode="rb") as public_key_file:
        public_key = public_key_file.read()
    with open(private_key_path, mode="rb") as private_key_file:
        private_key = private_key_file.read()

    cert = x509.load_pem_x509_certificate(public_key)
    fingerprint = cert.fingerprint(hashes.SHA1())
    kid = fingerprint.hex()
    x5t = base64.urlsafe_b64encode(fingerprint).decode("utf-8")

    payload = {
        "jti": str(uuid.uuid4()),
        "iat": datetime.datetime.utcnow(),
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=5),
        "aud": "http://gpcs.hishsp.com",
        "RequestBranchIdentifier": "gainwell-de-training",
    }

    headers = {"x5t": x5t, "kid": kid}
    auth_token = jwt.encode(payload, private_key, algorithm="RS256", headers=headers)
    return auth_token


def generate_bearer_token(
    public_key_path: str, private_key_path: str, base_url: str, path: str
) -> str:
    """Generates a JWT Bearer token for GPCS."""
    assertion = generate_assertion(public_key_path, private_key_path)
    try:
        response = requests.post(
            f"{base_url}/path",
            json={"grant_type": "jwt-bearer", "assertion": assertion},
            timeout=60,
        )
        response.raise_for_status()
        print(response.json().get("access_token"))
        return response.json().get("access_token")
    except HTTPError as e:
        raise Exception("STS Authentication Error", e)


def get_cert_from_url(
    hostname: str, port: int = 443, cert_file: str = "server_cert.pem"
) -> None:
    """Fetches the SSL certificate from the specified hostname and saves it in PEM format."""
    context = ssl.create_default_context()

    try:
        with socket.create_connection((hostname, port), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                der_cert = ssock.getpeercert(binary_form=True)
                pem_cert = ssl.DER_cert_to_PEM_cert(der_cert)

                with open(cert_file, "w", encoding="utf-8") as f:
                    f.write(pem_cert)

                print(f"Certificate saved to '{cert_file}'")
    except (socket.error, ssl.SSLError) as e:
        print(f"Failed to retrieve certificate from {hostname}:{port} - {e}")


def main():
    get_cert_from_url(
        hostname="api.openweathermap.org", port=443, cert_file="weather.pem"
    )


if __name__ == "__main__":
    main()
