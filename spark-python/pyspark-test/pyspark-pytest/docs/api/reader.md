# CSV Reader

A simple CSV reader utility that loads CSV files into PySpark DataFrames.

## Source

```python title="src/reader/spark_reader.py"
--8<-- "src/reader/spark_reader.py"
```

## Usage

```python
from reader.spark_reader import load_csv

df = load_csv(spark, "path/to/file.csv")
```

## Options

The reader is configured with:

| Option | Value | Description |
| --- | --- | --- |
| `sep` | `,` | Column separator |
| `inferSchema` | `true` | Auto-detect column types |
| `header` | `true` | First row contains column names |

## Testing

The reader is tested using **mock-based testing** — no actual CSV files needed:

```python
from unittest.mock import Mock
from reader.spark_reader import load_csv

mock_spark = Mock()
mock_df = Mock()
mock_spark.read.format.return_value = mock_spark
mock_spark.option.return_value = mock_spark
mock_spark.load.return_value = mock_df

result = load_csv(mock_spark, "test.csv")
mock_spark.load.assert_called_with("test.csv")
```

## Run Tests

```bash
uv run pytest tests/reader/ -v
```
