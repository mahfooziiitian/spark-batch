"""Demonstrate exporting an inferred XML schema for reuse.

After reading XML with schema inference, the detected schema can be
saved as JSON or DDL for use in production pipelines. This avoids
repeated inference on every read. This script shows:
- Infer schema from a sample XML file
- Export schema as JSON file (StructType.jsonValue())
- Export schema as DDL string (StructType.toDDL())
- Reload exported schema and read XML with it
"""

import json
import os
import sys
import textwrap
from pathlib import Path

from pyspark.sql.types import StructType

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

SENSORS_XML = textwrap.dedent(
    """\
    <?xml version="1.0" encoding="UTF-8"?>
    <sensors>
      <reading>
        <sensor_id>TEMP-001</sensor_id>
        <location>Building A</location>
        <temperature>22.5</temperature>
        <humidity>45.2</humidity>
        <pressure>1013.25</pressure>
        <timestamp>2024-11-20T10:30:00</timestamp>
        <battery_pct>87</battery_pct>
        <active>true</active>
      </reading>
      <reading>
        <sensor_id>TEMP-002</sensor_id>
        <location>Building B</location>
        <temperature>19.8</temperature>
        <humidity>52.1</humidity>
        <pressure>1012.80</pressure>
        <timestamp>2024-11-20T10:30:05</timestamp>
        <battery_pct>63</battery_pct>
        <active>true</active>
      </reading>
      <reading>
        <sensor_id>TEMP-003</sensor_id>
        <location>Warehouse</location>
        <temperature>15.3</temperature>
        <humidity>60.0</humidity>
        <pressure>1014.10</pressure>
        <timestamp>2024-11-20T10:30:10</timestamp>
        <battery_pct>12</battery_pct>
        <active>false</active>
      </reading>
    </sensors>
"""
)


def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    data_home = os.environ.get("DATA_HOME", "/tmp")
    base = Path(data_home) / "file_data" / "xml" / "schema_demo"

    xml_path = base / "sensors.xml"
    json_schema_path = base / "sensors_schema.json"
    ddl_schema_path = base / "sensors_schema.ddl"
    write_file(xml_path, SENSORS_XML)

    spark = get_spark_session(
        app_name="spark-xml-schema-export",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    # --- 1. Infer schema ---
    print("=== Step 1: Infer Schema ===")
    df_inferred = (
        spark.read.format("xml").option("rowTag", "reading").load(xml_path.as_posix())
    )
    inferred_schema = df_inferred.schema
    df_inferred.printSchema()

    # --- 2. Export as JSON ---
    print("=== Step 2: Export Schema as JSON ===")
    schema_json = json.dumps(inferred_schema.jsonValue(), indent=2)
    write_file(json_schema_path, schema_json)
    print(f"Saved JSON schema → {json_schema_path}")
    print(schema_json[:300] + "...")

    # --- 3. Export as DDL-like string ---
    print("\n=== Step 3: Export Schema as SimpleString ===")
    schema_simple = inferred_schema.simpleString()
    write_file(ddl_schema_path, schema_simple)
    print(f"Saved schema string → {ddl_schema_path}")
    print(schema_simple)

    # --- 4. Reload JSON schema and read ---
    print("\n=== Step 4: Reload JSON Schema ===")
    with open(json_schema_path) as f:
        reloaded_schema = StructType.fromJson(json.load(f))

    df_from_json = (
        spark.read.format("xml")
        .option("rowTag", "reading")
        .schema(reloaded_schema)
        .load(xml_path.as_posix())
    )
    df_from_json.show(truncate=False)

    # --- 5. Reload DDL schema and read ---
    print("=== Step 5: Reload via JSON (DDL not available in PySpark <4) ===")
    with open(json_schema_path) as f:
        schema_reloaded_again = StructType.fromJson(json.load(f))
    df_from_reloaded = (
        spark.read.format("xml")
        .option("rowTag", "reading")
        .schema(schema_reloaded_again)
        .load(xml_path.as_posix())
    )
    df_from_reloaded.show(truncate=False)

    # --- 6. Verify schemas match ---
    print("=== Step 6: Schema Equality Check ===")
    assert reloaded_schema == inferred_schema, "JSON round-trip schema mismatch!"
    print("✓ JSON schema matches original")

    spark.stop()
