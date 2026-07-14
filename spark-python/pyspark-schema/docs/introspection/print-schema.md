# Print Schema

## Output Formats

Given a DataFrame with a nested struct, PySpark offers multiple ways to
inspect the schema:

=== "printSchema()"
    ```text
    root
     |-- rollno: string (nullable = false)
     |-- name: string (nullable = true)
     |-- metrics: struct (nullable = true)
     |    |-- age: integer (nullable = true)
     |    |-- height: float (nullable = true)
     |    |-- weight: integer (nullable = true)
     |-- address: string (nullable = true)
    ```

=== "simpleString()"
    ```text
    struct<rollno:string,name:string,metrics:struct<age:int,height:float,weight:int>,address:string>
    ```

=== "dtypes"
    ```python
    [('rollno', 'string'), ('name', 'string'),
     ('metrics', 'struct<age:int,height:float,weight:int>'),
     ('address', 'string')]
    ```

=== "json()"
    ```json
    {
      "type": "struct",
      "fields": [
        {"name": "rollno", "type": "string", "nullable": false, "metadata": {}},
        ...
      ]
    }
    ```

## Accessing Nested Field Metadata

```python
# All top-level fields
[f.name for f in df.schema.fields]

# Nested fields of a struct column
df.schema["metrics"].dataType.fieldNames()

# Type and nullable of a specific field
field = df.schema["metrics"]
print(field.dataType.simpleString(), field.nullable)
```

## JSON Schema Serialisation

```python title="src/arrays/pyspark_array_schema_json.py"
--8<-- "src/arrays/pyspark_array_schema_json.py"
```

## Print Columns Example

```python title="src/column/print_columns.py"
--8<-- "src/column/print_columns.py"
```

## Run

```bash
SPARK_MASTER=local[*] python src/arrays/pyspark_array_schema_print.py
SPARK_MASTER=local[*] python src/column/print_columns.py
```

## Key Points

- `df.printSchema()` is the fastest way to visually verify a schema at the REPL.
- `simpleString()` is compact enough for log messages.
- Use `schema.json()` when storing schema snapshots or sending to a registry.
- `df.schema.fields` gives full `StructField` objects including metadata.
