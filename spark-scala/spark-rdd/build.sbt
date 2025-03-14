ThisBuild / organization := "com.mahfooz.spark"
ThisBuild /version := "1.0"
ThisBuild /scalaVersion := "2.12.18"
name := "spark-rdd"
Global /resolvers += Resolver.mavenLocal

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-streaming" % "3.3.2",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)
def getProject(projectName:String) : Project = Project(projectName,file(projectName))

val sparkRddShuffling = getProject("spark-rdd-shuffling")
val sparkRddSerde = getProject("spark-rdd-serde")
val sparkRddPipe = getProject("spark-rdd-pipe")
val sparkRddType = getProject("spark-rdd-type")
val sparkRddPartition = getProject("spark-rdd-partition")
val sparkRddDatasource = getProject("spark-rdd-datasource")
val sparkRddCheckpoint = getProject("spark-rdd-checkpoint")
val sparkRddDataLocality = getProject("spark-rdd-data-locality")
val sparkRddCaching = getProject("spark-rdd-caching")
val sparkRddAccumulator = getProject("spark-rdd-accumulator")
val sparkRddBroadcast = getProject("spark-rdd-broadcast")
val sparkRddDataframe = getProject("spark-rdd-dataframe")
val sparkRddArchitecture = getProject("spark-rdd-architecture")
val sparkRddOperation = getProject("spark-rdd-operation")
val sparkRddOptimization = getProject("spark-rdd-optimization")
val sparkRddDataset = getProject("spark-rdd-dataset")
val sparkRddCollection = getProject("spark-rdd-collection")
val sparkRddUtil = getProject("spark-rdd-util")
val sparkRddWorkflow = getProject("spark-rdd-workflow")
