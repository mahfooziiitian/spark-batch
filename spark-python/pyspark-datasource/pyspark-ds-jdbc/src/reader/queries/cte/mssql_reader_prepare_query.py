import os

from utils.config_reader import ConfigReader
from utils.spark_util import get_spark_session

if __name__ == '__main__':
    mysql_version = '8.0.32'
    packages = [
        f'com.mysql:mysql-connector-j:{mysql_version}'
    ]

    configs = {
        "spark.app.name": "MySparkApp",
        "spark.master": "local[*]",
        "spark.jars.packages": ",".join(packages)
    }
    spark = get_spark_session(configs)
    db_config = os.environ['DATA_HOME'] + "\\Database\\Config\\MySQL\\db.conf"
    config_reader = ConfigReader(db_config)

    source_table = "kafka_offset"
    reader_properties = {
        "user": config_reader.get_user(),
        "password": config_reader.get_password(),
        "driver": config_reader.get_driver()
    }

    df = (spark.read.format("jdbc")
          .option("url", config_reader.get_url())
          .option("prepareQuery", "(SELECT * INTO #TempTable FROM (SELECT * FROM tbl) t)")
          .option("query", "SELECT * FROM #TempTable")
          .load())
