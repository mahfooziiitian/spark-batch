name := "spark-docker"

version := "1.0"

scalaVersion := "2.11.12"

idePackagePrefix := Some("com.mahfooz.spark.docker")

lazy val sparkDockerBase=project in file("spark-docker-base")
lazy val sparkDockerMaster=project in file("spark-docker-master")
lazy val sparkDockerWorker=project in file("spark-docker-worker")
lazy val sparkDockerSubmit=project in file("spark-docker-submit")

lazy val sparkDockerTemplate=project in file("spark-docker-template")