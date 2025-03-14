/*
docker run \
  --net=host \
  --rm \
  confluentinc/cp-enterprise-kafka:4.0.0  \
  kafka-topics --delete --topic jdbc-tb_movenl_trf_cdrs --zookeeper mtmdevhdoped01:32181

 */
package com.mahfooz.spark.kafka.confluent

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import za.co.absa.abris.examples.utils.ExamplesUtils._

object ConfluentKafkaAvroReader {

  private val PARAM_JOB_NAME = "job.name"
  private val PARAM_JOB_MASTER = "job.master"
  private val PARAM_PAYLOAD_AVRO_SCHEMA = "payload.avro.schema"
  private val PARAM_LOG_LEVEL = "log.level"
  private val PARAM_OPTION_SUBSCRIBE = "option.subscribe"

  private val PARAM_SHOULD_USE_SCHEMA_REGISTRY = "should.use.schema.registry"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    checkArgs(args)

    val properties = loadProperties(args)

    val spark = getSparkSession(
      properties,
      PARAM_JOB_NAME,
      PARAM_JOB_MASTER,
      PARAM_LOG_LEVEL
    )

    val stream = spark.readStream
      .format("kafka")
      .option("startingOffsets", "latest")
      .addOptions(properties)

    // 1. this method will add the properties starting with "option."
    // 2. security options can be set in the properties file

    val deserialized = configureExample(stream.load(), properties)

    // YOUR OPERATIONS CAN GO HERE

    deserialized.printSchema()

    deserialized.writeStream
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "c:/tmp/abris")
      .start()
      .awaitTermination()
  }

  private def configureExample(
      dataFrame: DataFrame,
      properties: Properties
  ): Dataset[Row] = {

    import za.co.absa.abris.avro.functions.from_confluent_avro

    val schemaRegistryConfig =
      properties.getSchemaRegistryConfigurations(PARAM_OPTION_SUBSCRIBE)

    if (properties.getProperty(PARAM_SHOULD_USE_SCHEMA_REGISTRY).toBoolean) {
      dataFrame.select(
        from_confluent_avro(col("value"), schemaRegistryConfig) as 'data
      )
    } else {
      val source = scala.io.Source.fromFile(
        properties.getProperty(PARAM_PAYLOAD_AVRO_SCHEMA)
      )
      val schemaString =
        try source.mkString
        finally source.close()
      dataFrame.select(
        from_confluent_avro(
          col("value"),
          schemaString,
          schemaRegistryConfig
        ) as 'data
      )
    }
  }
}
