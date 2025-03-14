/*

Pivot tables are an essential part of data analysis and reporting.
Many popular data manipulation tools (pandas, reshape2, and Excel) and databases (MS SQL and Oracle 11g) include the ability to pivot
data.

The PIVOT clause is used for data perspective.
We can get the aggregated values based on specific column values, which will be turned to multiple columns used in SELECT clause.
The PIVOT clause can be specified after the table name or subquery.

PIVOT ( { aggregate_expression [ AS aggregate_expression_alias ] } [ , ... ]
    FOR column_list IN ( expression_list ) )

aggregate_expression

  Specifies an aggregate expression (SUM(a), COUNT(DISTINCT b), etc.).

aggregate_expression_alias

  Specifies an alias for the aggregate expression.

column_list

  Contains columns in the FROM clause, which specifies the columns we want to replace with new columns.
  We can use brackets to surround the columns, such as (c1, c2).

expression_list

  Specifies new columns, which are used to match values in column_list as the aggregating condition.
  We can also add aliases for them.

 */
package com.mahfooz.spark.sql.aggregation.pivoting

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object PivotTable {

  def main(args: Array[String]): Unit = {

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("PivotTable")
      .getOrCreate()

    val data=Seq(Row("foo",	"one","small",1),
      Row("foo",	"one","large",2),
      Row("foo",	"one","large",2),
      Row("foo",	"two","small",3),
      Row("foo",	"two","small",3),
      Row("bar",	"one","large",4),
      Row("bar",	"one","small",5),
      Row("bar",	"two","small",6),
      Row("bar",	"two","large",7))

    val schema= StructType(
      Array(StructField("A",StringType, true),
      StructField("B",StringType, true),
      StructField("C",StringType, true),
      StructField("D",IntegerType, true)))

    val df=spark.createDataFrame(spark.sparkContext.parallelize(data),schema)

    //we wanted to group by two columns A and B, pivot on column C, and sum column D
    df.groupBy("A", "B")
      .pivot("C", Seq("small", "large"))
      .sum("D")
      .show()
  }
}
