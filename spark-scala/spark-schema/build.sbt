name := "spark-schema"

ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.17"
ThisBuild / organization := "com.mahfooz.spark.schema"

resolvers in Global += Resolver.mavenLocal

resolvers in Global += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots"
)

val sparkVersion = "3.3.2"
val sparkHiveVersion = "3.3.2"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

def getProject(name:String,location:String) : Project = Project(name,file(location))
def getProject(name:String) : Project = Project(name,file(name))

lazy val sparkSchemaUtil = getProject("spark-schema-util")
lazy val sparkSchemaAutomatic = getProject("spark-schema-automatic").dependsOn(sparkSchemaUtil)
lazy val sparkSchemaData = getProject("spark-schema-data").dependsOn(sparkSchemaUtil)
lazy val sparkSchemaManual = getProject("spark-schema-manual").dependsOn(sparkSchemaUtil)
lazy val sparkSchemaTable = getProject("spark-schema-table").dependsOn(sparkSchemaUtil)
lazy val sparkSchemaValidation = getProject("spark-schema-validation").dependsOn(sparkSchemaUtil)
lazy val sparkSchemaReflection = getProject("spark-schema-reflection").dependsOn(sparkSchemaUtil)
lazy val sparkSchemaTypes = getProject("spark-schema-types").dependsOn(sparkSchemaUtil)
lazy val sparkSchemaNested = getProject("spark-schema-nested").dependsOn(sparkSchemaUtil)
lazy val sparkSchemaDdl = getProject("spark-schema-ddl").dependsOn(sparkSchemaUtil)
lazy val sparkSchemaCases = getProject("spark-schema-cases").dependsOn(sparkSchemaUtil)
lazy val sparkSchemaMerging = getProject("spark-schema-merging").dependsOn(sparkSchemaUtil)
lazy val sparkSchemaEncoder = getProject("spark-schema-encoder").dependsOn(sparkSchemaUtil)

