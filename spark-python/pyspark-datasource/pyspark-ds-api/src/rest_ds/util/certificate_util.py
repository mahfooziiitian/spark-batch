import logging
import socket
import ssl

# Note: JWT assertion / bearer-token generation for OAuth2 certificate-based
# auth now lives in `rest_ds.authentication.auth_util` (used by the
# `oauth2_assertion` flow), which is the parameterized, actively used
# implementation. The hardcoded, unused duplicates that used to live here
# were removed as part of the duplicate-code cleanup.

logger = logging.getLogger(__name__)


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

                logger.info("Certificate saved to '%s'", cert_file)
    except (socket.error, ssl.SSLError) as e:
        logger.error(
            "Failed to retrieve certificate from %s:%s - %s", hostname, port, e
        )
