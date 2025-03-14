package com.mahfooz.spark.catalog

import org.apache.spark.sql.SparkSession

object SparkCatalog {

  def main(args: Array[String]): Unit = {
    val sparkWarehouse =
      sys.env.getOrElse("SPARK_WAREHOUSE", "spark-warehouse").replace("\\", "/")
    System.setProperty(
      "derby.system.home",
      sys.env.getOrElse("derby.system.home", "").replace("\\", "/")
    )

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkManagedTable")
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()

    //Accessing Catalog
    val catalog = spark.catalog

    //Querying the databases
    catalog.listDatabases().select("name").show()

    import spark.implicits._

    //Registering Dataframe with createTempView
    val df = Seq(
      (1,"malam",20),
      (2,"halam",3),
      (3,"hfatma",3)
    ).toDF("id","name","age")

    df.createTempView("employee")

    //Querying the tables
    catalog.listTables().select("name").show(false)

    //Checking is table cached or not
    println(catalog.isCached("employee"))

    df.cache()
    println(catalog.isCached("employee"))

    //Drop view
    catalog.dropTempView("employee")
    catalog.listTables().select("name").show(false)
  }

}
