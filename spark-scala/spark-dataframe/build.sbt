ThisBuild / organization := "com.mahfooz.spark"
ThisBuild / version := "1.0"
name := "spark-dataframe"

ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.4.0"
val scalaTestVersion = "3.2.16"

Global / resolvers += Resolver.mavenLocal

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)
lazy val dataframeAggregation =
  Project("dataframe-aggregation", file("dataframe-aggregation"))

lazy val dataframeColumn = Project("dataframe-column", file("dataframe-column"))
  .dependsOn(dataframeUtil)

lazy val dataframeDs =
  Project("dataframe-ds", file("dataframe-ds")).dependsOn(dataframeUtil)

lazy val dataframeDsl = Project("dataframe-dsl", file("dataframe-dsl"))

lazy val dataframeExpression =
  Project("dataframe-expression", file("dataframe-expression"))

lazy val dataframeUtil = Project("dataframe-util", file("dataframe-util"))

lazy val dataframeJoin =
  Project("dataframe-join", file("dataframe-join")).dependsOn(dataframeModel)

lazy val dataframeOptimization =
  Project("dataframe-optimization", file("dataframe-optimization"))

val dataframeModel = Project("dataframe-model", file("dataframe-model"))

val dataframePartition =
  Project("dataframe-partition", file("dataframe-partition"))

val dataframePersistence =
  Project("dataframe-persistence", file("dataframe-persistence"))

val dataframeSchema = Project("dataframe-schema", file("dataframe-schema"))
  .dependsOn(dataframeUtil)

val dataframeAction =
  Project("spark-dataframe-action", file("spark-dataframe-action"))
val dataframeReaderWriter =
  Project("dataframe-reader-writer", file("dataframe-reader-writer"))
val dataframeTransformation =
  Project("dataframe-transformation", file("dataframe-transformation"))
val dataframeConversion =
  Project("dataframe-conversion", file("dataframe-conversion"))

val dataframeWrangling =
  Project("dataframe-wrangling", file("dataframe-wrangling"))

val dataframeArchitecture =
  Project("dataframe-architecture", file("dataframe-architecture"))

val dataframeUdf =
  Project("dataframe-udf", file("dataframe-udf"))
