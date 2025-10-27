export SPARK_HOME=/usr/local/spark
export PATH=$SPARK_HOME/bin:$PATH
export JAVA_HOME=$JAVA_HOME_17

pyspark --master local[4] --conf "spark.executor.memory=2g" --conf "spark.driver.memory=2g"