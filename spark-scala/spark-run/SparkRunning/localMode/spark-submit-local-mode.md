# running application in local mode

    $SPARK_HOME/bin/spark-submit \
        --class com.databricks.example.DataFrameExample \
        --master local \
        target/scala-2.11/example_2.11-0.1-SNAPSHOT.jar \
        "hello"

