import os

from utils.config_reader import ConfigReader
from utils.spark_util import get_spark_session

if __name__ == '__main__':
    mysql_version = '8.0.33'
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

    data_frame = spark.createDataFrame([
        (1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Charlie", 22)
    ], ["id", "name", "age"])

    # Write the data to a JDBC table
    (data_frame.write
     .option("user", config_reader.get_user())
     .option("driver", config_reader.get_driver())
     .option("password", config_reader.get_password())
     .jdbc(url=config_reader.get_url(), table="people", mode="overwrite"))
