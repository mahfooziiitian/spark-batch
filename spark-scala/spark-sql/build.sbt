name := "spark-sql"

ThisBuild / version := "1.0"
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.spark.sql"

def getProject(name: String): Project = getProject(name, name)
def getProject(name: String, path: String): Project = Project(name, file(path))
val sparkSqlUtil = getProject("spark-sql-util")
val sparkSqlAde = getProject("spark-sql-ade")
val sparkSqlDependency = getProject("spark-sql-dependency")
val sparkSqlConfig = getProject("spark-sql-config").dependsOn(sparkSqlUtil)
val sparkSqlDml = getProject("spark-sql-dml").dependsOn(sparkSqlUtil)
val sparkSqlSql = getProject("spark-sql-sql").dependsOn(sparkSqlUtil)
val sparkSqlAggregation = getProject("spark-sql-aggregation").dependsOn(sparkSqlUtil)
val sparkSqlJoin = getProject("spark-sql-join").dependsOn(sparkSqlUtil)
lazy val sparkSqlHive = getProject("spark-sql-hive").dependsOn(sparkSqlUtil)
val sqlFramework = getProject("spark-sql-framework")
val sqlFilter = getProject("spark-sql-filter").dependsOn(sparkSqlUtil)
val sqlCaching = getProject("spark-sql-caching").dependsOn(sparkSqlUtil)

lazy val sparkSqlUdf = getProject("spark-sql-udf").dependsOn(sparkSqlUtil)
lazy val sparkSqlWindow = getProject("spark-sql-window").dependsOn(sparkSqlUtil)
lazy val sparkSqlColumn = getProject("spark-sql-column")
lazy val sparkSqPartition = getProject("spark-sql-partition").dependsOn(sparkSqlUtil)
lazy val sparkSqlSet = getProject("spark-sql-set")
lazy val sparkSqlCatalog = getProject("spark-sql-catalog").dependsOn(sparkSqlUtil)