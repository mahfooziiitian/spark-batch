"""custom_ds — reusable helper library for PySpark 4 Python Data Source API examples.

Exposes the SparkSession helper, custom DataSource implementations, and a
one-call registration helper so examples and tests stay concise.
"""

from __future__ import annotations

from custom_ds.batch.simple_source import SimpleDataSource
from custom_ds.restapi.oauth import OAuth2Config
from custom_ds.restapi.rest_arrow_source import RestApiArrowDataSource
from custom_ds.restapi.rest_source import RestApiDataSource
from custom_ds.restapi.rest_stream_source import RestApiStreamDataSource
from custom_ds.restapi.rest_stream_writer import RestApiStreamSinkDataSource
from custom_ds.restapi.rest_writer import RestApiSinkDataSource
from custom_ds.restapi.uc_connection import UCConnectionConfig
from custom_ds.session import create_dbconnect_session, create_spark_session
from custom_ds.streaming.simple_stream_source import SimpleStreamDataSource
from custom_ds.uc_auth.weather_source import WeatherApiSource
from custom_ds.util.registration import register_all
from custom_ds.writer.simple_writer import SimpleSinkDataSource

__all__ = [
    "OAuth2Config",
    "RestApiArrowDataSource",
    "RestApiDataSource",
    "RestApiSinkDataSource",
    "RestApiStreamDataSource",
    "RestApiStreamSinkDataSource",
    "SimpleDataSource",
    "SimpleSinkDataSource",
    "SimpleStreamDataSource",
    "UCConnectionConfig",
    "WeatherApiSource",
    "create_dbconnect_session",
    "create_spark_session",
    "register_all",
]
