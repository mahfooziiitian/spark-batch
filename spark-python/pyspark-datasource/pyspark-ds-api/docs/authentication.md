# Authentication

Every authentication strategy lives under `examples/authentication/` as a
self-contained scenario: a mock API server, an ETL/runner script, and a YAML
config. All strategies dispatch through the single library entry point
`rest_ds.authentication.auth_util.get_auth_headers()`, so adding a new auth
type to a YAML config never requires touching ingestion code.

| Strategy | Example path | Notes |
|---|---|---|
| HTTP Basic | `examples/authentication/basic/` | `Authorization: Basic base64(user:pass)` |
| Bearer / JWT | `examples/authentication/jwt/` | RS256-signed assertions |
| API Key (header) | `examples/authentication/api_key/header/` | Custom header, e.g. `X-API-Key` |
| API Key (query param) | `examples/authentication/api_key/query/` | `?apikey=...` |
| mTLS | `examples/authentication/mtls/` | Client certificate + CA verification |
| OAuth2 — client credentials | `examples/authentication/oauth2/client_credential/` | Basic, form, and JSON token-request variants |
| OAuth2 — password grant | `examples/authentication/oauth2/password/` | Form and JSON variants |
| OAuth2 — JWT assertion | `examples/authentication/oauth2/assertion/` | RFC 7523 `urn:ietf:params:oauth:grant-type:jwt-bearer` |
| OAuth2 — authorization code | `examples/authentication/oauth2/authorization_code/` | Browser redirect flow (FastAPI + session middleware) |
| OAuth2 — device code | `examples/authentication/oauth2/device_code/` | Device authorization grant |
| Certificate generation | `examples/authentication/certificates/` | Self-signed cert/CA generation + validation utilities |

## Running an example

```bash
# 1. Start the scenario's mock API server (each has its own port)
PYTHONPATH=src uv run python examples/authentication/basic/api_basic_source.py &

# 2. Run the matching ETL script, which reads the sibling YAML config
PYTHONPATH=src uv run python examples/authentication/basic/api_basic_authentication.py
```

## Library reference

See [`rest_ds.authentication.auth_util`][rest_ds.authentication.auth_util]
in the [API reference](reference.md) for the dispatcher signature, and
[`rest_ds.util.certificate_util`][rest_ds.util.certificate_util] for X.509
certificate helpers (assertion generation, bearer-token exchange, fetching a
live TLS cert from a hostname).
