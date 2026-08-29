"""REST API custom data sources — batch reader, batch writer, streaming reader/writer, Arrow, OAuth2, and UC connection."""

from __future__ import annotations

from custom_ds.restapi.oauth import OAuth2Config
from custom_ds.restapi.rest_arrow_source import RestApiArrowDataSource
from custom_ds.restapi.rest_source import RestApiDataSource
from custom_ds.restapi.rest_stream_source import RestApiStreamDataSource
from custom_ds.restapi.rest_stream_writer import RestApiStreamSinkDataSource
from custom_ds.restapi.rest_writer import RestApiSinkDataSource
from custom_ds.restapi.uc_connection import UCConnectionConfig

__all__ = [
    "OAuth2Config",
    "RestApiArrowDataSource",
    "RestApiDataSource",
    "RestApiSinkDataSource",
    "RestApiStreamDataSource",
    "RestApiStreamSinkDataSource",
    "UCConnectionConfig",
]
