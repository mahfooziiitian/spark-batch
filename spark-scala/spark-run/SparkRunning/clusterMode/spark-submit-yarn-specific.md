# YARN-only

| Option                | Description                                                                                                                                                                                                                                                           |
|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| --queue QUEUE_NAME    | The YARN queue to submit to (Default: "default").                                                                                                                                                                                                                     |
| --num-executors NUM   | Number of executors to launch (Default: 2). If dynamic allocation is enabled, the initial number of executors will be at least NUM.                                                                                                                                   |
| --archives ARCHIVES   | Comma separated list of archives to be extracted into the working directory of each executor.                                                                                                                                                                         |
| --principal PRINCIPAL | Principal to be used to login to KDC, while running on secure HDFS.                                                                                                                                                                                                   |
| --keytab KEYTAB       | The full path to the file that contains the keytab for the principal specified above. This keytab will be copied to the node running the Application Master via the Secure Distributed Cache, for renewing the login tickets and the  delegation tokens periodically. |


# Examples


    spark-submit \
        --class com.mahfooz.spark.run.modes.cluster.yarn.RunYarnCluster \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 1g \
        --executor-memory 1g \
        --executor-cores 2 \
        spark-run_2.12-1.0.jar \


    spark-submit \
        --master yarn \
        --deploy-mode client \
        --executor-memory 1g \
        --class com.mahfooz.spark.run.modes.cluster.yarn.RunYarnCluster \
        --name RunYarnCluster \
        --conf "spark.app.id=RunYarnCluster"  \
        spark-run_2.12-1.0.jar \
        hdfs://10.133.43.95:8020/ \
        /user/hdfs/product.csv 2
