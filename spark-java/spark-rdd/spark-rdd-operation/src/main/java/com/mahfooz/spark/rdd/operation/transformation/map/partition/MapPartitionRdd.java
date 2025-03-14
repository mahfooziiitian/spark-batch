/*

Suppose you have elements from 1 to 100 distributed among 10 partitions i.e. 10 elements/partition.
map() transformation will call func 100 times to process these 100 elements but in case of mapPartitions(), func will be called
once/partition i.e. 10 times.
Secondly, mapPartitions() holds the data in-memory i.e. it will store the result in memory until all the elements of the partition has been
processed.
mapPartitions() will return the result only after it finishes processing of whole partition.
mapPartitions() requires an iterator input unlike map() transformation.

 */
package com.mahfooz.spark.rdd.operation.transformation.map.partition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class MapPartitionRdd {

    public static void main(String[] args) {
        SparkConf sparkConf=new SparkConf()
                .setMaster("local[*]")
                .setAppName("MapPartitionRdd");
        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
        javaSparkContext.parallelize(Arrays.asList(new Integer []{1,2,3,4,5,6,7,8,9,10}), 3)
                .map(x->new Tuple2<Integer,String>(x, "Hello")).collect().forEach(x-> System.out.println(x));

        javaSparkContext.parallelize(Arrays.asList(new Integer []{1,2,3,4,5,6,7,8,9,10}), 3)
                .mapPartitions(x->Arrays.asList("Hello").iterator()).collect().forEach(x-> System.out.println(x));

        javaSparkContext.parallelize(Arrays.asList(new Integer []{1,2,3,4,5,6,7,8,9,10}), 3)
                .mapPartitions(x->Arrays.asList(x.next()).iterator()).collect().forEach(x-> System.out.println(x));

        javaSparkContext.stop();
    }
}
