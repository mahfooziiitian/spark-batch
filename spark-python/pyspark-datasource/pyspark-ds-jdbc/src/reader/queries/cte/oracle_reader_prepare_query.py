import os

from utils.config_reader import ConfigReader
from utils.spark_util import get_spark_session

if __name__ == '__main__':
    oracle_version = "23.5.0.24.07"
    packages = [f"com.oracle.database.jdbc:ojdbc8:{oracle_version}"]

    configs = {
        "spark.app.name": "MySparkApp",
        "spark.master": "local[*]",
        "spark.jars.packages": ",".join(packages)
    }
    spark = get_spark_session(configs)
    db_config = os.environ['DATA_HOME'] + "\\Database\\Config\\Oracle\\db.conf"
    config_reader = ConfigReader(db_config)

    source_table = "kafka_offset"
    reader_properties = {
        "user": config_reader.get_user(),
        "password": config_reader.get_password(),
        "driver": config_reader.get_driver()
    }

    prepare_query_df = (spark.read.format("jdbc")
                        .option("url", config_reader.get_url())
                        .option("prepareQuery", "WITH t AS (SELECT topic_name, batch_id FROM kafka_config_test)")
                        .option("query", "SELECT * FROM t WHERE batch_id > 1")
                        .options(**reader_properties)
                        .load())

    prepare_query_df.show(truncate=False)
