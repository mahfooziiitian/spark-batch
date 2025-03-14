"""
createTableColumnTypes

    The database column data types to use instead of the defaults, when creating the table.
    Data type information should be specified in the same format as CREATE TABLE columns syntax
    (e.g: "name CHAR(64), comments VARCHAR(1024)").
    The specified types should be valid spark sql data types.
"""
import os

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from utils.config_reader import ConfigReader
from utils.spark_util import get_spark_session

if __name__ == '__main__':
    mysql_version = '8.0.32'
    packages = [
        f'com.mysql:mysql-connector-j:{mysql_version}'
    ]

    configs = {
        "spark.app.name": "create_column_type",
        "spark.master": "local[*]",
        "spark.jars.packages": ",".join(packages)
    }
    spark = get_spark_session(configs)
    db_config = os.environ['DATA_HOME'] + "\\Database\\Config\\MySQL\\db.conf"
    config_reader = ConfigReader(db_config)

    source_table = "create_column_type"
    reader_properties = {
        "user": config_reader.get_user(),
        "password": config_reader.get_password(),
        "driver": config_reader.get_driver()
    }

    jdbc_url = config_reader.get_url()

    schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True)
    ])

    data = [(1, "John Doe", 30), (2, "Jane Doe", 25)]

    df = spark.createDataFrame(data, schema=schema)

    (df.write
     .option("createTableColumnTypes", "id int, name varchar(100), age int")
     .jdbc(url=jdbc_url, table=source_table, mode="overwrite", properties=reader_properties)
     )
