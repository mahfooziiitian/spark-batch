package com.mahfooz.sparksql.schema.nested

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object NestedSchemaRead {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("NestedSchemaRead")
      .master("local[*]")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val dataDS = Seq(
      """{
         "dc_id": "dc-101",
         "source": {
         "sensor-igauge": {
         "id": 10,
         "ip": "68.28.91.22",
         "description": "Sensor attached to the container ceilings",
          "temp":35,
          "c02_level": 1475,
          "geo": {"lat":38.00, "long":97.00}
        },
        "sensor-ipad": {
          "id": 13,
          "ip": "67.185.72.1",
          "description": "Sensor ipad attached to carbon cylinders",
          "temp": 34,
          "c02_level": 1370,
          "geo": {"lat":47.41, "long":-122.00}
        },
        "sensor-inest": {
          "id": 8,
          "ip": "208.109.163.218",
          "description": "Sensor attached to the factory ceilings",
          "temp": 40,
          "c02_level": 1346,
          "geo": {"lat":33.61, "long":-111.89}
        },
        "sensor-istick": {
          "id": 5,
          "ip": "204.116.105.67",
          "description": "Sensor embedded in exhaust pipes in the ceilings",
          "temp": 40,
          "c02_level": 1574,
          "geo": {"lat":35.93, "long":-85.46}
        }
      }
      }""").toDS()

    // should only be one item
    dataDS.count()

    val schema = new StructType()
      .add("dc_id", StringType) // data center where data was posted to Kafka cluster
      .add("source", // info about the source of alarm
        MapType( // define this as a Map(Key->value)
          StringType,
          new StructType()
            .add("description", StringType)
            .add("ip", StringType)
            .add("id", LongType)
            .add("temp", LongType)
            .add("c02_level", LongType)
            .add("geo",
              new StructType()
                .add("lat", DoubleType)
                .add("long", DoubleType)
            )
        )
      )

    val df = spark // spark session
      .read // get DataFrameReader
      .schema(schema) // use the defined schema above and read format as JSON
      .json(dataDS)

    df.printSchema

    val explodedDF = df.select($"dc_id", explode($"source"))

    explodedDF.printSchema()

  }
}
