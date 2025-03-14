package org.apache.spark

import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

object RecoverCheckpoint {

  def recover[T: ClassTag](sc: SparkContext, path: String): RDD[T] = {
    sc.checkpointFile[T](path)
  }
}
