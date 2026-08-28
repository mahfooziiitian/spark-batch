"""Unit tests for `rest_ds.util.config_validator`, which validates a loaded
YAML config's shape/required-fields upfront (auth type, pagination
strategy, response format), before any HTTP request is attempted."""

import pytest

from rest_ds.util.config_validator import ConfigValidationError, validate_config


def _config(options, location="https://api.example.com/data"):
    return {
        "extracts": {
            "extract": {
                "source": {"params": {"location": location, "options": options}}
            }
        }
    }


def test_valid_none_auth_config_passes():
    validate_config(_config({"authentication": {"type": "none"}}))


def test_valid_basic_auth_config_passes():
    config = _config(
        {"authentication": {"type": "basic", "username": "u", "password": "p"}}
    )
    validate_config(config)


def test_valid_apikey_header_name_value_config_passes():
    config = _config(
        {
            "authentication": {
                "type": "apikey",
                "in": "header",
                "name": "X-API-Key",
                "value": "secret",
            }
        }
    )
    validate_config(config)


def test_valid_apikey_query_alt_convention_config_passes():
    config = _config(
        {
            "authentication": {
                "type": "apikey",
                "in": "query",
                "api_key_name": "api_key",
                "api_key_value": "secret",
            }
        }
    )
    validate_config(config)


def test_valid_pagination_config_passes():
    config = _config(
        {
            "authentication": {"type": "none"},
            "pagination": {"strategy": "offset", "limit": 10},
        }
    )
    validate_config(config)


def test_missing_extracts_section_raises():
    with pytest.raises(ConfigValidationError, match="extracts.extract.source.params"):
        validate_config({})


def test_missing_location_raises():
    config = _config({"authentication": {"type": "none"}}, location=None)
    with pytest.raises(ConfigValidationError, match="location"):
        validate_config(config)


def test_missing_options_raises():
    config = {
        "extracts": {
            "extract": {
                "source": {"params": {"location": "https://api.example.com/data"}}
            }
        }
    }
    with pytest.raises(ConfigValidationError, match="options"):
        validate_config(config)


def test_unsupported_auth_type_raises():
    config = _config({"authentication": {"type": "totally_made_up"}})
    with pytest.raises(ConfigValidationError, match="totally_made_up"):
        validate_config(config)


def test_basic_auth_missing_password_raises():
    config = _config({"authentication": {"type": "basic", "username": "u"}})
    with pytest.raises(ConfigValidationError, match="password"):
        validate_config(config)


def test_oauth2_client_credentials_missing_fields_raises():
    config = _config(
        {"authentication": {"type": "oauth2_client_credentials", "tokenUrl": "u"}}
    )
    with pytest.raises(ConfigValidationError, match="clientId"):
        validate_config(config)


def test_apikey_missing_in_raises():
    config = _config({"authentication": {"type": "apikey", "name": "k", "value": "v"}})
    with pytest.raises(ConfigValidationError, match="authentication.in"):
        validate_config(config)


def test_apikey_missing_name_value_pair_raises():
    config = _config({"authentication": {"type": "apikey", "in": "header"}})
    with pytest.raises(ConfigValidationError, match="api_key_name"):
        validate_config(config)


def test_unsupported_pagination_strategy_raises():
    config = _config(
        {
            "authentication": {"type": "none"},
            "pagination": {"strategy": "made_up_strategy"},
        }
    )
    with pytest.raises(ConfigValidationError, match="made_up_strategy"):
        validate_config(config)


def test_unsupported_response_format_raises():
    config = _config({"authentication": {"type": "none"}, "responseFormat": "yaml"})
    with pytest.raises(ConfigValidationError, match="responseFormat"):
        validate_config(config)


def test_multiple_errors_are_all_reported():
    config = _config(
        {
            "authentication": {"type": "basic"},
            "pagination": {"strategy": "bogus"},
        }
    )
    with pytest.raises(ConfigValidationError) as exc_info:
        validate_config(config)
    message = str(exc_info.value)
    assert "username" in message
    assert "password" in message
    assert "bogus" in message
