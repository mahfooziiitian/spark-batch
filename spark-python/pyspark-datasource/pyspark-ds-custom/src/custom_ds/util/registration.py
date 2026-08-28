"""Registration helper for all custom_ds data sources."""

from __future__ import annotations

from pyspark.sql import SparkSession

from custom_ds.batch.simple_source import SimpleDataSource
from custom_ds.restapi.rest_arrow_source import RestApiArrowDataSource
from custom_ds.restapi.rest_source import RestApiDataSource
from custom_ds.restapi.rest_stream_source import RestApiStreamDataSource
from custom_ds.restapi.rest_stream_writer import RestApiStreamSinkDataSource
from custom_ds.restapi.rest_writer import RestApiSinkDataSource
from custom_ds.streaming.simple_stream_source import SimpleStreamDataSource
from custom_ds.uc_auth.weather_source import WeatherApiSource
from custom_ds.writer.simple_writer import SimpleSinkDataSource

ALL_DATA_SOURCES = (
    SimpleDataSource,
    SimpleSinkDataSource,
    SimpleStreamDataSource,
    RestApiDataSource,
    RestApiArrowDataSource,
    RestApiSinkDataSource,
    RestApiStreamDataSource,
    RestApiStreamSinkDataSource,
    WeatherApiSource,
)


def register_all(spark: SparkSession) -> None:
    """Register every custom data source in this library on the given session."""
    for data_source in ALL_DATA_SOURCES:
        spark.dataSource.register(data_source)
