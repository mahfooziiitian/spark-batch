# Spark 2
    
    spark-submit \
        --conf "spark.executor.extraJavaOptions:" \
        --conf "spark.driver.extraJavaOption: -Dlog4j.configuration=file:///etc/user/spark/master.log4j.properties"

