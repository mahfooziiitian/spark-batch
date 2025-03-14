import os

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

    df = (spark.read.format("jdbc")
          .option("url", config_reader.get_url())
          .option("dbtable", "people")
          .option("user", config_reader.get_user())
          .option("password", config_reader.get_password())
          .option("driver", config_reader.get_driver())
          .load())

    df.show(truncate=False)

