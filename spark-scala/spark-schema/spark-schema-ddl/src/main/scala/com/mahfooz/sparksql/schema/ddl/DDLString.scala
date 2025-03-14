package com.mahfooz.sparksql.schema.ddl

import org.apache.spark.sql.types.StructType

object DDLString {

  def main(args: Array[String]): Unit = {

    val ddlSchemaStr = "`fullName` STRUCT<`first`: STRING, `last`: STRING,`middle`: STRING>,`age` INT,`gender` STRING"
    val ddlSchema = StructType.fromDDL(ddlSchemaStr)
    ddlSchema.printTreeString()

  }
}
