# Transformations

DataFrame text transformation utilities.

## Source

```python title="src/transformation/df_transformation.py"
--8<-- "src/transformation/df_transformation.py"
```

## Functions

### remove_extra_spaces

Collapses multiple consecutive whitespace characters into a single space.

```python
from transformation.df_transformation import remove_extra_spaces

cleaned_df = remove_extra_spaces(df, "name")
```

| Input | Output |
| --- | --- |
| `"John    D."` | `"John D."` |
| `"Alice   G."` | `"Alice G."` |
| `"Bob  T."` | `"Bob T."` |

## Testing

Tested using PySpark's built-in `assertDataFrameEqual`:

```python
from pyspark.testing import assertDataFrameEqual
from transformation.df_transformation import remove_extra_spaces

original_df = spark.createDataFrame([{"name": "John    D.", "age": 30}])
transformed_df = remove_extra_spaces(original_df, "name")

expected_df = spark.createDataFrame([{"name": "John D.", "age": 30}])
assertDataFrameEqual(transformed_df, expected_df)
```

## Run Tests

```bash
uv run pytest tests/transformation/ -v
```
