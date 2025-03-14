import os

from utils.config_reader import ConfigReader
from utils.spark_util import get_spark_session
from pyspark.sql import functions as F, Window

if __name__ == '__main__':
    configs = {
        "spark.app.name": "MySparkApp",
        "spark.master": "local[*]",
        "spark.jars.packages": "com.oracle.database.jdbc:ojdbc11:23.2.0.0"
    }
    spark = get_spark_session(configs)
    db_config = os.environ['DATA_HOME'] + "\\Database\\Config\\Oracle\\db.conf"
    config_reader = ConfigReader(db_config)

    source_table = "c##dc.magna_cdc_table_cp_metrics"
    properties = {
        "user": config_reader.get_user(),
        "password": config_reader.get_password(),
        "driver": config_reader.get_driver(),
        "fetchsize": "1000"
    }

    jdbc_url = config_reader.get_url()

    df = spark.read.jdbc(url=jdbc_url, table=source_table, properties=properties)
    df.show()

    window_spec = Window.partitionBy("JOB_ID").orderBy(df["ID"].desc())

    df = df.select("*", F.rank().over(window_spec).alias("rank")).where("rank==1")

    write_properties = {
        "user": config_reader.get_user(),
        "password": config_reader.get_password(),
        "driver": config_reader.get_driver(),
        "batchsize": "1000",
        "isolationLevel": "READ_COMMITTED"
    }

    destination_table = "c##dc.magna_cdc_table_cp_metrics_latest"
    df.write.jdbc(url=jdbc_url, table=destination_table, mode="append", properties=write_properties)
