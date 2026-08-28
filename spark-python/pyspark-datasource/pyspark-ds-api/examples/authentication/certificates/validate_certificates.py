"""Validate mTLS client/CA certificates and probe a secure API endpoint with them."""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests
from cryptography import x509
from cryptography.hazmat.backends import default_backend

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("DATA_HOME", "/tmp")) / "rest_api_ds" / "certs"
DEFAULT_CLIENT_CERT = os.environ.get("MTLS_CLIENT_CERT", str(DATA_DIR / "client.pem"))
DEFAULT_CLIENT_KEY = os.environ.get("MTLS_CLIENT_KEY", str(DATA_DIR / "client.key"))
DEFAULT_CA_BUNDLE = os.environ.get("MTLS_CA_BUNDLE", str(DATA_DIR / "ca.pem"))
DEFAULT_URL = os.environ.get(
    "MTLS_TEST_URL", "https://your-api-server.example.com/secure-endpoint"
)
EXPIRY_WARNING_DAYS = 30


@dataclass
class CertificateInfo:
    """Summary of a parsed X.509 certificate."""

    path: Path
    subject: str
    issuer: str
    not_before: datetime
    not_after: datetime

    @property
    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > self.not_after

    @property
    def days_until_expiry(self) -> int:
        return (self.not_after - datetime.now(timezone.utc)).days


def load_certificate(cert_path: Path) -> CertificateInfo:
    """Parse a PEM-encoded certificate and return its key details.

    Args:
        cert_path: Path to a PEM certificate file.

    Returns:
        Parsed certificate metadata (subject, issuer, validity window).

    Raises:
        FileNotFoundError: If the certificate file does not exist.
        ValueError: If the file cannot be parsed as a PEM certificate.
    """
    if not cert_path.is_file():
        raise FileNotFoundError(f"Certificate file not found: {cert_path}")

    try:
        cert = x509.load_pem_x509_certificate(cert_path.read_bytes(), default_backend())
    except ValueError as parse_err:
        raise ValueError(
            f"Unable to parse certificate '{cert_path}': {parse_err}"
        ) from parse_err

    return CertificateInfo(
        path=cert_path,
        subject=cert.subject.rfc4514_string(),
        issuer=cert.issuer.rfc4514_string(),
        not_before=cert.not_valid_before_utc,
        not_after=cert.not_valid_after_utc,
    )


def check_certificate(cert_path: Path, label: str) -> bool:
    """Log certificate details and validity, returning False on problems."""
    try:
        info = load_certificate(cert_path)
    except (FileNotFoundError, ValueError) as err:
        logger.error("%s: %s", label, err)
        return False

    logger.info("%s: subject=%s issuer=%s", label, info.subject, info.issuer)
    logger.info(
        "%s: valid %s -> %s",
        label,
        info.not_before.isoformat(),
        info.not_after.isoformat(),
    )

    if info.is_expired:
        logger.error("%s: certificate expired on %s", label, info.not_after.isoformat())
        return False

    if info.days_until_expiry <= EXPIRY_WARNING_DAYS:
        logger.warning(
            "%s: certificate expires in %d day(s)", label, info.days_until_expiry
        )

    return True


def call_secure_endpoint(
    url: str,
    client_cert: Path,
    client_key: Path | None,
    ca_bundle: Path,
    timeout: int = 30,
) -> requests.Response:
    """Call `url` presenting the client certificate and verifying against `ca_bundle`."""
    cert = (str(client_cert), str(client_key)) if client_key else str(client_cert)
    response = requests.get(url, cert=cert, verify=str(ca_bundle), timeout=timeout)
    response.raise_for_status()
    return response


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-cert", default=DEFAULT_CLIENT_CERT, type=Path)
    parser.add_argument("--client-key", default=DEFAULT_CLIENT_KEY, type=Path)
    parser.add_argument("--ca-bundle", default=DEFAULT_CA_BUNDLE, type=Path)
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument(
        "--skip-request",
        action="store_true",
        help="Only validate certificate files locally; do not call the endpoint.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    client_ok = check_certificate(args.client_cert, "client certificate")
    ca_ok = check_certificate(args.ca_bundle, "CA bundle")

    if not (client_ok and ca_ok):
        logger.error("Certificate validation failed; aborting.")
        return 1

    if args.skip_request:
        logger.info("Skipping live request (--skip-request set).")
        return 0

    try:
        response = call_secure_endpoint(
            args.url, args.client_cert, args.client_key, args.ca_bundle, args.timeout
        )
        logger.info("Response status: %s", response.status_code)
        logger.info("Response body: %s", response.text)
        return 0
    except requests.exceptions.SSLError as ssl_err:
        logger.error("SSL error: %s", ssl_err)
    except requests.exceptions.RequestException as req_err:
        logger.error("Request failed: %s", req_err)
    return 1


if __name__ == "__main__":
    sys.exit(main())
