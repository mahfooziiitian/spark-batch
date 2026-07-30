# DDL String Schema

Define schemas using Spark's compact DDL string notation.

## Usage

```python title="examples/06_schema/02_ddl_string_schema.py"
--8<-- "examples/06_schema/02_ddl_string_schema.py"
```

!!! tip
    DDL strings are concise and readable for simple schemas:
    ```python
    schema = "name STRING, age INT, city STRING"
    df = spark.read.schema(schema).json("data.json")
    ```

## Run

```bash
python examples/06_schema/02_ddl_string_schema.py
```
