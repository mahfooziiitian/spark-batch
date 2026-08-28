# API key authentication

- `header/` — `x-api-key` header, validated by a local mock server
  (`api_key_header_source.py`). Uses a non-secret fixture key
  (`API_KEY_HEADER_DEMO_KEY`, defaults to `demo-local-fixture-key`).
- `query/` — `appid` query param sent to the real OpenWeatherMap API. Set
  `OPENWEATHERMAP_API_KEY` in your environment before running
  `api_key_query.py` — get a free key at https://openweathermap.org/api.
