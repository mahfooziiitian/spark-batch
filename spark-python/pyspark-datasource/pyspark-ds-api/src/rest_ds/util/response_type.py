"""Supported REST API response formats.

Centralizes the `responseFormat` values consumed by `rest_ds.rest_api`
(`APIClient`, `read_api`) and `rest_ds.util.api_client.make_request` /
`rest_ds.util.data_processor.read_api`, so both implementations validate
against the same single source of truth instead of duplicating an inline
tuple of magic strings that could silently drift out of sync.

Other common REST content types this framework does not yet natively
support (kept here for reference):

- application/xml – used for XML data; less common than JSON.
- text/html – used for HTML data; common for web pages.
- application/x-www-form-urlencoded – used for form submissions.
- multipart/form-data – used for file uploads.
- application/octet-stream – used for binary data / file downloads.
"""

from enum import Enum


class ResponseFormat(str, Enum):
    """Supported values for the `responseFormat` YAML config option."""

    JSON = "json"
    TEXT = "text"
    XML = "xml"
    CSV = "csv"


SUPPORTED_RESPONSE_FORMATS = frozenset(fmt.value for fmt in ResponseFormat)

# Formats handled as "not JSON" by the callers above (raw response.text).
RAW_TEXT_RESPONSE_FORMATS = frozenset(
    fmt.value for fmt in ResponseFormat if fmt is not ResponseFormat.JSON
)
