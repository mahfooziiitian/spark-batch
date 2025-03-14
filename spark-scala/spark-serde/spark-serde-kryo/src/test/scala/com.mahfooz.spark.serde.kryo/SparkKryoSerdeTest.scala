package com.mahfooz.spark.serde.kryo

import java.io.File

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.util.Utils

class SparkKryoSerdeTest {
}
