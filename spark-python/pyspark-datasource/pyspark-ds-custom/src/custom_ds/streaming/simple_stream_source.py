"""A simple counter streaming source demonstrating the streaming Data Source API.

Reference: pyspark.sql.datasource.{DataSource, SimpleDataSourceStreamReader}
"""

from __future__ import annotations

from collections.abc import Iterator

from pyspark.sql.datasource import DataSource, SimpleDataSourceStreamReader
from pyspark.sql.types import StructType


class SimpleStreamDataSource(DataSource):
    """A minimal streaming source that emits an incrementing counter.

    Options:
        rowsPerBatch (int): number of rows to emit per micro-batch. Default 2.

    Usage:
        spark.readStream.format("simple_stream").option("rowsPerBatch", 5).load()
    """

    @classmethod
    def name(cls) -> str:
        return "simple_stream"

    def schema(self) -> str:
        return "value LONG"

    def simpleStreamReader(self, schema: StructType) -> SimpleDataSourceStreamReader:
        return SimpleCounterStreamReader(self.options)


class SimpleCounterStreamReader(SimpleDataSourceStreamReader):
    def __init__(self, options: dict) -> None:
        self.rows_per_batch = int(options.get("rowsPerBatch", 2))

    def initialOffset(self) -> dict:
        return {"offset": 0}

    def read(self, start: dict) -> tuple[Iterator[tuple], dict]:
        start_offset = start["offset"]
        end_offset = start_offset + self.rows_per_batch
        rows = [(i,) for i in range(start_offset, end_offset)]
        return iter(rows), {"offset": end_offset}

    def readBetweenOffsets(self, start: dict, end: dict) -> Iterator[tuple]:
        return iter([(i,) for i in range(start["offset"], end["offset"])])
