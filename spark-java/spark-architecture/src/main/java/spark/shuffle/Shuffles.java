/*

A shuffle redistributes data among a cluster of nodes.

It is an expensive operation because it involves moving data across a network.

Note that a shuffle does not randomly redistribute data; it groups data elements into buckets based on some criteria.

Each bucket forms a new partition.

 */
package spark.shuffle;

public class Shuffles {
}
