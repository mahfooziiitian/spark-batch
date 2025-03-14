name := "spark-serde"

resolvers in Global += Resolver.mavenLocal

scalaVersion := "2.11.8"


resolvers ++= Seq(
  "Typesafe" at "https://repo.typesafe.com/typesafe/releases/",
  "Java.net Maven2 Repository" at "https://download.java.net/maven/2/",
  "Apache Snapshot Repository" at "https://repository.apache.org/snapshots",
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.0-cdh6.3.3",
  "org.apache.spark" %% "spark-core" % "2.4.0-cdh6.3.3",
  "org.apache.spark" %% "spark-streaming" % "2.4.0-cdh6.3.3"
)

def getProject(name:String,path: String) : Project = Project(name,file(path))
def getProject(name:String) : Project = getProject(name,name)

lazy val sparkSerdeKryo=getProject("spark-serde-kryo")
lazy val sparkCodec=getProject("spark-codec")