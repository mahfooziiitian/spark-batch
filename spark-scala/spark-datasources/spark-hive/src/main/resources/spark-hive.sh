#Running spark shell in yarn client mode
#Launch the Spark Shell on a YARN cluster
./bin/spark-shell \
  --num-executors 2 \
  --executor-memory 512m \
  --master yarn-client


spark-shell \
  --conf spark.hadoop.hive.metastore.warehouse.dir=/tmp/hive-warehouse

