"""In-memory batch data source demonstrating the Python Data Source API.

Reference: pyspark.sql.datasource.{DataSource, DataSourceReader, InputPartition}
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType


@dataclass
class RangePartition(InputPartition):
    """One partition worth of generated rows: rows [start, end)."""

    start: int
    end: int


class SimpleDataSource(DataSource):
    """A minimal, partitioned, in-memory batch data source.

    Options:
        numRows (int): total number of rows to generate. Default 10.
        numPartitions (int): number of partitions to split rows into. Default 2.

    Usage:
        spark.read.format("simple").option("numRows", 100).load()
    """

    @classmethod
    def name(cls) -> str:
        return "simple"

    def schema(self) -> str:
        return "id LONG, value STRING"

    def reader(self, schema: StructType) -> DataSourceReader:
        return SimpleDataSourceReader(schema, self.options)


class SimpleDataSourceReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict) -> None:
        self.schema = schema
        self.num_rows = int(options.get("numRows", 10))
        self.num_partitions = max(1, int(options.get("numPartitions", 2)))

    def partitions(self) -> list[InputPartition]:
        base, remainder = divmod(self.num_rows, self.num_partitions)
        result: list[InputPartition] = []
        start = 0
        for i in range(self.num_partitions):
            size = base + (1 if i < remainder else 0)
            if size == 0:
                continue
            result.append(RangePartition(start, start + size))
            start += size
        return result or [RangePartition(0, 0)]

    def read(self, partition: RangePartition):
        for i in range(partition.start, partition.end):
            yield (i, f"row-{i}")
