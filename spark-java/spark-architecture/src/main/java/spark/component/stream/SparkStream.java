/*

While data is arriving continuously in an unbounded sequence is what we call a data stream.

Basically, for further processing, Streaming divides continuous flowing input data into discrete units.

Moreover, we can say it is a low latency processing and analyzing of streaming data.

we can do data ingestion from many sources. Such as Kafka, Apache Flume, Amazon Kinesis or TCP sockets.

It is an add-on to core Spark API which allows scalable, high-throughput, fault-tolerant stream processing of live data streams.

Spark can access data from sources like Kafka, Flume, Kinesis or TCP socket.

 Spark uses Micro-batching for real-time streaming.
 
 Hence Spark Streaming, groups the live data into small batches. It then delivers it to the batch system for processing.
 
 Micro-batching is a technique that allows a process or task to treat a stream as a sequence of small batches of data.


How does Spark Streaming Works?

There are 3 phases of Spark Streaming:

a. GATHERING

	The Spark Streaming provides two categories of built-in streaming sources:

    Basic sources: These are the sources which are available in the StreamingContext API.
    	
    	Examples: file systems, and socket connections.
    
    Advanced sources: These are the sources like Kafka, Flume, Kinesis, etc. are available through extra utility classes.
    		Hence Spark access data from different sources like Kafka, Flume, Kinesis, or TCP sockets.

b. PROCESSING

	The gathered data is processed using complex algorithms expressed with a high-level function.
	
	For example, map, reduce, join and window.

c. DATA STORAGE
	
	The Processed data is pushed out to file systems, databases, and live dashboards.

	Spark Streaming also provides high-level abstraction. It is known as discretized stream or DStream.

	DStream in Spark signifies continuous stream of data.
	
	We can form DStream in two ways either from sources such as Kafka, Flume, and Kinesis or by high-level operations on other DStreams. Thus, DStream is internally a sequence of RDDs.


 */
package spark.component.stream;

public class SparkStream {

}
