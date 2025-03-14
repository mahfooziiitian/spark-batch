/*

val sc: SparkContext // An existing SparkContext.

val sqlContext = new org.apache.spark.sql.SQLContext(sc)


val df = sqlContext.read.json("home/spark/input.json")

input.registerTempTable("students")

val teenagers = sqlContext.sql("SELECT name, age FROM students WHERE age >= 13 AND age <= 19")



scala> import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext

scala> val sqlContext=new SQLContext(sc) ;
warning: there was one deprecation warning; re-run with -deprecation for details
sqlContext: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext@35ff072c

scala> sqlContext.read.json("Students.json");
res0: org.apache.spark.sql.DataFrame = [age: string, name: string]

scala> val stdout=sqlContext.read.json("Students.json");
stdout: org.apache.spark.sql.DataFrame = [age: string, name: string]

scala> stdout.show
+---+----+
|age|name|
+---+----+
| 23| Ben|
| 22|Alen|
+---+----+


scala> stdout.registerTempTable("students")
warning: there was one deprecation warning; re-run with -deprecation for details

scala> sqlContext.sql("select * from students where age=22")
res3: org.apache.spark.sql.DataFrame = [age: string, name: string]

scala> val studata=sqlContext.sql("select * from students where age=22")
studata: org.apache.spark.sql.DataFrame = [age: string, name: string]

scala> studata.show
+---+----+
|age|name|
+---+----+
| 22|Alen|
+---+----+

 */
package spark.api.v1.context.sql;

public class SqlContexts {

}
