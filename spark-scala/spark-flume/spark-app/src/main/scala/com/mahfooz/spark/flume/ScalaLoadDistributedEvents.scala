package com.mahfooz.spark.flume
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.flume._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd._
import org.apache.spark.streaming.dstream._
import java.net.InetSocketAddress
import java.io.ObjectOutputStream
import java.io.ObjectOutput

object ScalaLoadDistributedEvents {

  def main(args: Array[String]) {

    println("Creating Spark Configuration")

    //Create an Object of Spark Configuration
    val conf = new SparkConf()

    //Set the logical and user defined Name of this //Application
    conf.setAppName("Streaming Data Loading Application")

    println("Retreiving Streaming Context from Spark Conf")

    //Second parameter is the time interval at which streaming data will be divided into batches
    val streamCtx = new StreamingContext(conf, Seconds(2))

    //Create an Array of InetSocketaddress containing the Host and the Port of the machines
    //where Flume Sink is delivering the Events
    var addresses = new Array[InetSocketAddress](2);
    addresses(0) = new InetSocketAddress("localhost", 4949)
    addresses(1) = new InetSocketAddress("localhost", 4950)

    //Create a Flume Polling Stream which will poll the Sink the get the events from sinks every 2 seconds.
    //1.maxBatchSize = It is the maximum number of events to be pulled from the Spark sink in a single RPC call.
    //2.parallelism - The Number of concurrent requests this stream should send to the sink.

    val flumeStream = FlumeUtils.createPollingStream(streamCtx, addresses,
      StorageLevel.MEMORY_AND_DISK_SER_2, 1000, 1)

    //Define Output Stream Connected to Console for //printing the results
    val outputStream = new ObjectOutputStream(Console.out)
    //Invoking custom Print Method for writing Events to //Console
    printValues(flumeStream, streamCtx, outputStream)

    //Most important statement which will initiate the //Streaming Context
    streamCtx.start();
    //Wait till the execution is completed.
    streamCtx.awaitTermination();
  }

  /**
   * Simple Print function, for printing all elements of RDD
   */
  def printValues(stream: DStream[SparkFlumeEvent],
                  streamCtx: StreamingContext,
                  outputStream: ObjectOutput) {
    stream.foreachRDD(foreachFunc)
    def foreachFunc = (rdd: RDD[SparkFlumeEvent]) => {
      val array = rdd.collect()
      println("---------Start Printing Results----------")
      println("Total size of Events= " + array.size)
      for (flumeEvent <- array) {
        val payLoad = flumeEvent.event.getBody()
        println(new String(payLoad.array()))
      }
      println("---------Finished Printing Results--------")
    }
  }
}