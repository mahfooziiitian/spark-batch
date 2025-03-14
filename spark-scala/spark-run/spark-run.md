# spark-sumit format as given below.

    ./bin/spark-submit \
        --class <main-class> \
        --master <master-url> \
        --deploy-mode <deploy-mode> \
        --conf <key>=<value> \
        ... # other options
        <application-jar-or-script> \
        [application-arguments]


# Running executor memory and total number of executors
    ./bin/spark-submit \
        --class org.apache.spark.examples.SparkPi \
        --master spark://207.184.161.138:7077 \
        --executor-memory 20G \
        --total-executor-cores 100 \
        replace/with/path/to/examples.jar \
        1000



# Deployment strategies
    
