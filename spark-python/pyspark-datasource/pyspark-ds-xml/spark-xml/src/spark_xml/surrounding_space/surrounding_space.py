"""Demonstrate spark-xml ignoreSurroundingSpaces option.

Generates sample XML with intentional leading/trailing whitespace in
element values, then reads it twice — with and without the
ignoreSurroundingSpaces option — to show the difference.
"""

import os
import sys
import textwrap
from pathlib import Path

from spark_xml.util.session.spark_session_util import get_spark_session

os.environ["JAVA_HOME"] = os.environ["JAVA_HOME_17"]
os.environ["PYSPARK_PYTHON"] = sys.executable

SAMPLE_XML = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <employees>
      <employee>
        <id>1</id>
        <name>   Alice Johnson   </name>
        <department>  Engineering  </department>
        <salary>  95000.50  </salary>
        <city>  New York  </city>
      </employee>
      <employee>
        <id>2</id>
        <name>  Bob Smith  </name>
        <department>   Marketing   </department>
        <salary>  82000.00  </salary>
        <city>   Chicago   </city>
      </employee>
      <employee>
        <id>3</id>
        <name>
          Carol Williams
        </name>
        <department>
          Finance
        </department>
        <salary>  105000.75  </salary>
        <city>  San Francisco  </city>
      </employee>
      <employee>
        <id>4</id>
        <name>  Dave Brown  </name>
        <department>  Engineering  </department>
        <salary>  88500.25  </salary>
        <city>  Austin  </city>
      </employee>
      <employee>
        <id>5</id>
        <name>   Eve Davis   </name>
        <department>  HR  </department>
        <salary>  76000.00  </salary>
        <city>   Boston   </city>
      </employee>
    </employees>
""")


def generate_sample_xml(path: Path) -> None:
    """Write the sample XML file with surrounding whitespace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(SAMPLE_XML, encoding="utf-8")
    print(f"Generated → {path}")


def read_xml(spark, path: str, ignore_spaces: bool):
    """Read XML with or without ignoreSurroundingSpaces."""
    return (
        spark.read.format("com.databricks.spark.xml")
        .option("rootTag", "employees")
        .option("rowTag", "employee")
        .option("ignoreSurroundingSpaces", str(ignore_spaces).lower())
        .load(path)
    )


if __name__ == "__main__":
    data_path = Path(os.environ["DATA_HOME"]) / "file_data" / "xml" / "employees_spaces.xml"
    generate_sample_xml(data_path)

    spark = get_spark_session(
        app_name="spark-xml-surrounding-spaces",
        scala_version="2.12",
        spark_xml_version="0.18.0",
    )

    path_str = data_path.as_posix()

    # --- Without ignoreSurroundingSpaces (default=true in spark-xml) ---
    print("\n=== ignoreSurroundingSpaces = false (preserve whitespace) ===")
    df_preserve = read_xml(spark, path_str, ignore_spaces=False)
    df_preserve.printSchema()
    df_preserve.show(truncate=False)

    # Show that values contain leading/trailing spaces
    from pyspark.sql.functions import col, length, trim

    print("--- String lengths vs trimmed lengths (spaces preserved) ---")
    df_preserve.select(
        col("name"),
        length(col("name")).alias("raw_len"),
        length(trim(col("name"))).alias("trimmed_len"),
    ).show(truncate=False)

    # --- With ignoreSurroundingSpaces = true (strips whitespace) ---
    print("\n=== ignoreSurroundingSpaces = true (strip whitespace) ===")
    df_stripped = read_xml(spark, path_str, ignore_spaces=True)
    df_stripped.show(truncate=False)

    print("--- String lengths vs trimmed lengths (spaces stripped) ---")
    df_stripped.select(
        col("name"),
        length(col("name")).alias("raw_len"),
        length(trim(col("name"))).alias("trimmed_len"),
    ).show(truncate=False)

    spark.stop()
