/*

Broadcast variables
Broadcast variables are shared variables across all executors. Broadcast variables are created once in the Driver and then are read only on executors. While it is simple to understand simple datatypes broadcasted, such as an Integer, broadcast is much bigger than simple variables conceptually. Entire datasets can be broadcasted in a Spark cluster so that executors have access to the broadcasted data. All the tasks running within an executor all have access to the broadcast variables.

Broadcast uses various optimized methods to make the broadcasted data accessible to all executors. This is an important challenge to solve as if the size of the datasets broadcasted is significant, you cannot expect 100s or 1000s of executors to connect to the Driver and pull the dataset. Rather, the executors pull the data via HTTP connection and the more recent addition which is similar to BitTorrent where the dataset itself is distributed like a torrent amongst the cluster. This enables a much more scalable method to distribute the broadcasted variables to all executors rather than having each executor pull the data from the Driver one by one which can cause failures on the Driver when you have a lot of executors.

The driver can only broadcast the data it has and you cannot broadcast RDDs by using references. This is because only Driver knows how to interpret RDDs and executors only know the particular partitions of data they are handling.
If you look deeper into how broadcast works, you will see that the mechanism works by first having the Driver divide the serialized object into small chunks and then stores those chunks in the BlockManager of the driver. When the code is serialized to be run on the executors, then each executor first attempts to fetch the object from its own internal BlockManager. If the broadcast variable was fetched before, it will find it and use it. However, if it does not exist, the executor then uses remote fetches to fetch the small chunks from the driver and/or other executors if available. Once it gets the chunks, it puts the chunks in its own BlockManager, ready for any other executors to fetch from. This prevents the driver from being the bottleneck in sending out multiple copies of the broadcast data (one per executor).

 */
package com.mahfooz.spark.rdd.broadcast.working

class BroadcastWorking {}
