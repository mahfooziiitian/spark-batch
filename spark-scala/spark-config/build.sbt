name := "spark-config"
ThisBuild / organization := "com.mahfooz.spark"
ThisBuild /version := "1.0"
ThisBuild /scalaVersion := "2.12.18"
Global /resolvers += Resolver.mavenLocal

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  Resolver.sonatypeRepo("public")
)
val sparkVersion = "3.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion
)
def getProject (filename: String) = Project(filename,file(filename))

val sparkConfigEnv= getProject("SparkConfigEnv")
val SparkShuffleBehavior= getProject("SparkShuffleBehavior")
val SparkConfigRuntime= getProject("SparkConfigRuntime")
val SparkConfigUI= getProject("SparkConfigUI")
val SparkConfigMemory= getProject("SparkConfigMemory")
val SparkConfigLogging= getProject("SparkConfigLogging")
val SparkConfigCompressSerde = getProject("SparkConfigCompressSerde")
val SparkApplicationConfig = getProject("SparkApplicationConfig")