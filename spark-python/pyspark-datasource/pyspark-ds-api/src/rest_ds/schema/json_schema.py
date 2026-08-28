import json

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType


def read_json_schema(file_path):
    """
    Reads a JSON schema file and returns the schema as a dictionary.

    :param file_path: Path to the JSON schema file
    :return: Dictionary representation of the JSON schema
    """
    with open(file=file_path, mode="r", encoding="utf-8") as f:
        schema_dict = json.load(f)
    # Convert JSON to Spark StructType
    return StructType.fromJson(schema_dict)


def generate_schema_from_df(json_schema_file: str, df: DataFrame):
    # Get schema as JSON string
    schema_json = df.schema.json()

    # Write schema JSON to a file
    with open(file=json_schema_file, mode="w", encoding="utf-8") as f:
        f.write(schema_json)
