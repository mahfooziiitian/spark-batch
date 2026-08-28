"""Demo: fetch a live TLS certificate from a hostname and save it to disk.

Run with: PYTHONPATH=src uv run python examples/util/demo_certificate_util.py
"""

from rest_ds.util.certificate_util import get_cert_from_url


def main():
    get_cert_from_url(
        hostname="api.openweathermap.org", port=443, cert_file="weather.pem"
    )


if __name__ == "__main__":
    main()
