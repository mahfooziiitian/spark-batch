import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("read_schema_in_json")
        .getOrCreate()
    )

    data_home = os.environ["DATA_HOME"]
    schema_path = f"{data_home}/file_data/xml/schema/notes_schema.json"
    xml_file = f"{data_home}/file_data/xml/notes.xml"

    with open(schema_path) as f:
        d = json.load(f)
        schema = StructType.fromJson(d)

    json_df = (
        spark.read.format("xml")
        .option("rowTag", "note")
        .option("excludeAttribute", "true")
        .option("ignoreNamespace", "true")
        .schema(schema)
        .load(xml_file)
    )

    json_df.show(truncate=False)
