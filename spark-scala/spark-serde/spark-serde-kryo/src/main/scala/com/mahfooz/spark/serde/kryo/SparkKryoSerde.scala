package com.mahfooz.spark.serde.kryo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

case class Person(name: String, age: Int)

object SparkKryoSerde {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("SparkKryoSerde")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .registerKryoClasses(
        Array(classOf[Person], classOf[Array[Person]],
          Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"))
      )

    val sparkContext = new SparkContext(conf)

    val personList: Array[Person] = (1 to 9999999)
      .map(value => Person("p"+value, value)).toArray

    //creating RDD of Person
    val rddPerson: RDD[Person] = sparkContext.parallelize(personList,5)
    val evenAgePerson: RDD[Person] = rddPerson.filter(_.age % 2 == 0)

    //persisting evenAgePerson RDD into memory
    evenAgePerson.persist(StorageLevel.MEMORY_ONLY_SER)

    evenAgePerson.take(50).foreach(x=>println(x.name,x.age))

  }
}
