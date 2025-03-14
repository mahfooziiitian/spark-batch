import os
from pathlib import PurePath

import findspark

if __name__ == '__main__':
    hiveMetastoreUri = PurePath(os.environ["DERBY_HOME"]).as_posix()
    dataWareHouse = PurePath(os.environ["SPARK_WAREHOUSE"]).as_posix()
    findspark.init()
    print(f"Hive metastore uri : {hiveMetastoreUri}")
    print(f"Spark dataWareHouse : {dataWareHouse}")


