"""Upfront validation for REST API ingestion YAML configs.

Config errors (a typo'd auth field, an unsupported pagination strategy,
etc.) previously only surfaced as a confusing `KeyError`/`AttributeError`
deep inside `auth_util.get_auth_headers` or `PaginationFactory`, often only
after the first HTTP request had already been attempted. `validate_config`
checks the whole config upfront (right after loading) and raises a single
`ConfigValidationError` listing every problem found, so misconfigurations
are caught immediately with an actionable message.
"""

from rest_ds.util.response_type import SUPPORTED_RESPONSE_FORMATS

# Required keys per `authentication.type`. `apikey` is validated separately
# below since its required keys depend on `in` (header vs. query) and on
# which of the two supported name/value key conventions is used.
_REQUIRED_AUTH_FIELDS = {
    "none": [],
    "basic": ["username", "password"],
    "bearer": ["token"],
    "mtls": ["certFile", "keyFile"],
    "oauth2_assertion": ["tokenUrl", "aud", "public_key_path", "private_key_path"],
    "oauth2_client_credentials": ["tokenUrl", "clientId", "clientSecret"],
    "oauth2_form_client_credentials": ["tokenUrl", "clientId", "clientSecret"],
    "oauth2_client_credentials_json": [
        "tokenUrl",
        "client_id_key",
        "client_id_value",
        "client_secret_key",
        "client_secret_value",
        "grant_type_key",
        "grant_type_value",
    ],
    "oauth2_client_credentials_form": [
        "tokenUrl",
        "client_id_key",
        "client_id_value",
        "client_secret_key",
        "client_secret_value",
        "grant_type_key",
        "grant_type_value",
    ],
    "oauth2_client_credentials_basic": [
        "tokenUrl",
        "client_id_value",
        "client_secret_value",
        "grant_type_key",
        "grant_type_value",
    ],
    "oauth2_password_form": [
        "tokenUrl",
        "username_key",
        "username_value",
        "password_key",
        "password_value",
        "grant_type_key",
        "grant_type_value",
    ],
    "oauth2_password_json": [
        "tokenUrl",
        "username_key",
        "username_value",
        "password_key",
        "password_value",
    ],
}

# `Paginator`/`PaginationFactory` required keys per `pagination.strategy`.
_REQUIRED_PAGINATION_FIELDS: dict = {
    "offset": [],
    "offset_page_token": [],
    "page": [],
    "cursor": [],
    "link": [],
}


class ConfigValidationError(ValueError):
    """Raised when a REST API ingestion YAML config fails validation."""


def _err(errors, message):
    errors.append(message)


def _validate_auth(auth_cfg, errors):
    if not auth_cfg:
        return
    auth_type = auth_cfg.get("type", "none")
    if auth_type == "apikey":
        _validate_apikey_auth(auth_cfg, errors)
        return
    if auth_type not in _REQUIRED_AUTH_FIELDS:
        _err(
            errors,
            f"authentication.type '{auth_type}' is not supported. "
            f"Supported types: {sorted(_REQUIRED_AUTH_FIELDS) + ['apikey']}",
        )
        return
    for key in _REQUIRED_AUTH_FIELDS[auth_type]:
        if not auth_cfg.get(key):
            _err(
                errors,
                f"authentication.type '{auth_type}' requires "
                f"'authentication.{key}' to be set.",
            )


def _validate_apikey_auth(auth_cfg, errors):
    location = auth_cfg.get("in")
    if location not in ("header", "query"):
        _err(
            errors,
            "authentication.type 'apikey' requires 'authentication.in' to "
            "be either 'header' or 'query'.",
        )
    has_name_value = auth_cfg.get("name") and auth_cfg.get("value")
    has_api_key_convention = auth_cfg.get("api_key_name") and auth_cfg.get(
        "api_key_value"
    )
    if not (has_name_value or has_api_key_convention):
        _err(
            errors,
            "authentication.type 'apikey' requires either "
            "'authentication.name'/'authentication.value' or "
            "'authentication.api_key_name'/'authentication.api_key_value' "
            "to be set.",
        )


def _validate_pagination(pagination_cfg, errors):
    if not pagination_cfg:
        return
    strategy = pagination_cfg.get("strategy")
    if strategy not in _REQUIRED_PAGINATION_FIELDS:
        _err(
            errors,
            f"pagination.strategy '{strategy}' is not supported. "
            f"Supported strategies: {sorted(_REQUIRED_PAGINATION_FIELDS)}",
        )


def _validate_options(opts, errors):
    response_format = opts.get("responseFormat", "json")
    if response_format not in SUPPORTED_RESPONSE_FORMATS:
        _err(
            errors,
            f"options.responseFormat '{response_format}' is not supported. "
            f"Supported formats: {sorted(SUPPORTED_RESPONSE_FORMATS)}",
        )
    _validate_auth(opts.get("authentication", {}), errors)
    _validate_pagination(opts.get("pagination", {}), errors)


def validate_config(config: dict) -> None:
    """Validates a loaded REST API ingestion YAML config, raising
    `ConfigValidationError` with every problem found if any are present.

    This only validates configuration shape/required-fields — it never
    makes network calls, so it is safe to run for every config load.
    """
    errors: list = []
    try:
        params = config["extracts"]["extract"]["source"]["params"]
    except (KeyError, TypeError):
        raise ConfigValidationError(
            "Config is missing the required 'extracts.extract.source.params' "
            "section."
        ) from None

    if not params.get("location"):
        _err(errors, "'extracts.extract.source.params.location' is required.")

    opts = params.get("options")
    if not opts:
        _err(errors, "'extracts.extract.source.params.options' is required.")
    else:
        _validate_options(opts, errors)

    if errors:
        formatted = "\n".join(f"  - {e}" for e in errors)
        raise ConfigValidationError(f"Invalid REST API ingestion config:\n{formatted}")
