import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from spark_xml.util.sample_data import ensure_notes_schema_json, ensure_notes_xml

os.environ["PYSPARK_PYTHON"] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("read_schema_in_json").getOrCreate()

    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    schema_path = str(ensure_notes_schema_json(data_home / "file_data" / "xml" / "schema" / "notes_schema.json"))
    xml_file = str(ensure_notes_xml(data_home / "file_data" / "xml" / "notes.xml"))

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
