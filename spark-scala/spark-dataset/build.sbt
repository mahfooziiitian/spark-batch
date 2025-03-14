ThisBuild / organization := "com.mahfooz.spark"
ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.11.12"
name := "spark-dataset"
Global / resolvers += Resolver.mavenLocal

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.0-cdh6.3.3",
  "org.apache.spark" %% "spark-core" % "2.4.0-cdh6.3.3",
  "org.apache.spark" %% "spark-streaming" % "2.4.0-cdh6.3.3",
  "org.scalatest" %% "scalatest" % "3.2.10" % Test
)

val datasetAggregation =
  Project("dataset-aggregation", file("dataset-aggregation"))
val datasetModel = Project("dataset-model",file("dataset-model"))
lazy val datasetCreation = Project("dataset-creation", file("dataset-creation")).dependsOn(datasetModel)
lazy val datasetEncoder = Project("dataset-encoder", file("dataset-encoder"))
  .dependsOn(datasetModel)
val datasetExpression = Project("dataset-expression", file("dataset-expression"))
val sparkDatasetArchitecture = Project("spark-dataset-architecture",file("spark-dataset-architecture"))
val sparkDatasetOptimization = Project("spark-dataset-optimization",file("spark-dataset-optimization"))
val datasetUtil = Project("dataset-util", file("dataset-util"))
  .dependsOn(datasetModel)
val sparkDatasetTyped = Project("spark-dataset-typed", file("spark-dataset-typed"))
  .dependsOn(datasetModel)
  .dependsOn(datasetUtil)
val sparkDatasetUntyped = Project("spark-dataset-untyped", file("spark-dataset-untyped")).dependsOn(datasetModel)
val datasetPartition = Project("dataset-partition", file("dataset-partition")).dependsOn(datasetModel)
val datasetColumn = Project("dataset-column", file("dataset-column")).dependsOn(datasetModel)
val datasetJoin = Project("dataset-join", file("dataset-join")).dependsOn(datasetModel)
