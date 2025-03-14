# Running spark application in cluster mode on yarn
    spark-submit  \
      --master yarn \
      --deploy-mode cluster \
      --driver-memory 1g \
      --executor-memory 1g \
      --executor-cores 2 \
      --class com.mahfooz.spark.run.modes.cluster.yarn.RunYarnCluster \
      spark-run_2.12-1.0.jar
      <app_args>



    spark-submit   \
        --class com.mahfooz.spark.build.WordCount  \
        --master yarn \
        --deploy-mode cluster \
        spark-build_2.10-1.0.jar

You can control the deployment mode of a Spark application

a)  Using spark-submit --deploy-mode command-line option

b)  spark.submit.deployMode Spark property.

      spark.submit.deployMode=client
      spark.submit.deployMode=cluster
