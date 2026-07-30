# JSON Schema to Spark

Convert standard JSON Schema files (draft-04/07/2020-12) to PySpark StructType schemas.

## Usage

```python title="examples/06_schema/10_json_schema_to_spark.py"
--8<-- "examples/06_schema/10_json_schema_to_spark.py"
```

## Type Mapping

| JSON Schema | PySpark |
|-------------|---------|
| `string` | `StringType` |
| `string` + `date` | `DateType` |
| `string` + `date-time` | `TimestampType` |
| `integer` | `LongType` |
| `integer` + `int32` | `IntegerType` |
| `number` | `DoubleType` |
| `number` + `float` | `FloatType` |
| `boolean` | `BooleanType` |
| `object` (properties) | `StructType` |
| `object` (additionalProperties) | `MapType` |
| `array` | `ArrayType` |

!!! tip "Usage Pattern"
    ```python
    from pys_json.schema import from_json_schema
    import json

    with open("my_schema.json") as f:
        spark_schema = from_json_schema(json.load(f))

    df = spark.read.schema(spark_schema).json("data.json")
    ```

## Run

```bash
python examples/06_schema/10_json_schema_to_spark.py
```
