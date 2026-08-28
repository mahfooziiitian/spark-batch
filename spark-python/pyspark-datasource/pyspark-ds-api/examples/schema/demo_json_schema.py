"""Demo: generate a Spark schema JSON file from a DataFrame, then read it back.

Run with: PYTHONPATH=src uv run python examples/schema/demo_json_schema.py
"""

import os
from pathlib import Path

from pyspark.sql import SparkSession

from rest_ds.schema.json_schema import generate_schema_from_df, read_json_schema


def main():
    # Create Spark session
    spark = SparkSession.builder.appName("SchemaToJson").getOrCreate()

    # Sample DataFrame
    data = [("Alice", 30), ("Bob", 25)]
    df = spark.createDataFrame(data, ["name", "age"])
    df.show()

    # Generate schema
    data_dir = Path(os.environ.get("DATA_HOME", "/tmp")) / "rest_api_ds"
    data_dir.mkdir(parents=True, exist_ok=True)
    json_schema_file = str(data_dir / "schema.json")
    generate_schema_from_df(json_schema_file, df)

    # Read schema from JSON file
    schema = read_json_schema(json_schema_file)
    print("Schema read from JSON file:")
    print(schema)

    # Create new dataframe
    df_schema = spark.createDataFrame(data, schema=schema)
    df_schema.show()


if __name__ == "__main__":
    main()
