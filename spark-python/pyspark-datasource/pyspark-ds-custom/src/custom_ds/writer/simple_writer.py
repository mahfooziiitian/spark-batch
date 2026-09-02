"""A simple JSON-lines file sink demonstrating the batch DataSourceWriter API.

Reference: pyspark.sql.datasource.{DataSource, DataSourceWriter, WriterCommitMessage}
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType

from custom_ds.util.log import get_logger

logger = get_logger(__name__)


@dataclass
class SimpleCommitMessage(WriterCommitMessage):
    partition_file: str
    num_rows: int


class SimpleSinkDataSource(DataSource):
    """A minimal batch sink that appends rows as JSON lines to a directory.

    Options:
        path (str): required. Directory to write JSON-line partition files into.

    Usage:
        df.write.format("simple_sink").option("path", "/tmp/out").mode("append").save()
    """

    @classmethod
    def name(cls) -> str:
        return "simple_sink"

    def schema(self) -> str:
        return "id LONG, value STRING"

    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        return SimpleSinkDataSourceWriter(self.options, overwrite)


class SimpleSinkDataSourceWriter(DataSourceWriter):
    def __init__(self, options: Mapping[str, str], overwrite: bool) -> None:
        self.path = options["path"]
        self.overwrite = overwrite

    def write(self, iterator: Iterator[Row]) -> SimpleCommitMessage:
        out_dir = Path(self.path)
        out_dir.mkdir(parents=True, exist_ok=True)
        partition_file = out_dir / f"part-{uuid.uuid4().hex}.jsonl"

        num_rows = 0
        with partition_file.open("w") as f:
            for row in iterator:
                f.write(json.dumps(row.asDict()) + "\n")
                num_rows += 1

        return SimpleCommitMessage(partition_file=str(partition_file), num_rows=num_rows)

    def commit(self, messages: list) -> None:
        total = sum(m.num_rows for m in messages if m is not None)
        logger.info("Committed %d rows across %d tasks", total, len(messages))

    def abort(self, messages: list) -> None:
        for message in messages:
            if message is not None:
                Path(message.partition_file).unlink(missing_ok=True)
