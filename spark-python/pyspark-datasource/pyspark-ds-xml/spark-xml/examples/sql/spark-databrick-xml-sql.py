import os
from pathlib import Path

from pyspark.sql import SparkSession

from spark_xml.util.sample_data import ensure_movies_xml

if __name__ == "__main__":
    spark_warehouse_dir = str(Path(os.environ.get("SPARK_WAREHOUSE", "/tmp/spark-warehouse")))
    Path(spark_warehouse_dir).mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.sql.warehouse.dir", spark_warehouse_dir)
        .appName("spark-db-xml")
        .getOrCreate()
    )
    data_home = Path(os.environ.get("DATA_HOME", "/tmp/spark-xml-data"))
    xml_uri = ensure_movies_xml(data_home / "file_data" / "xml" / "movies.xml").as_uri()
    spark.sql(f"""CREATE TABLE movies USING xml OPTIONS(path '{xml_uri}', rootTag 'collection', rowTag 'movie')""")

    spark.sql("select * from movies").show(truncate=False)
