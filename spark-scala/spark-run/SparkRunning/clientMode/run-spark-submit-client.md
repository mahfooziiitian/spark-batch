# Running spark in client mode
  
    spark-submit \
      --class com.mahfooz.spark.build.WordCount  \
      --master yarn \
      --deploy-mode client \
      spark-build_2.10-1.0.jar

    
       
    spark-submit \
      --master yarn \
      --deploy-mode client \
      --executor-memory 1g \
      --class com.mahfooz.spark.run.modes.cluster.yarn.RunYarnCluster \
      --name RunYarnCluster \
      --conf "spark.app.id=RunYarnCluster"  \
      spark-run_2.12-1.0.jar \
      hdfs://10.133.43.95:8020/ /user/hdfs/product.csv 2

