/*

The behavior of the above code is undefined, and may not work as intended.
To execute jobs, Spark breaks up the processing of RDD operations into tasks, each of which is executed by an executor.
Prior to execution, Spark computes the task’s closure.
The closure is those variables and methods which must be visible for the executor to perform its computations on the RDD.
This closure is serialized and sent to each executor.
The variables within the closure sent to each executor are now copies and thus, when counter is referenced within the foreach
function, it’s no longer the counter on the driver node.
There is still a counter in the memory of the driver node but this is no longer visible to the executors.
The executors only see the copy from the serialized closure.
Thus, the final value of counter will still be zero since all operations on counter were referencing the value within the
serialized closure.

In local mode, in some circumstances, the foreach function will actually execute within the same JVM as the driver and will
reference the same original counter, and may actually update it.
In general, closures - constructs like loops or locally defined methods, should not be used to mutate some global state.

 */
package com.mahfooz.spark.rdd.closure;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class RddClosure {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("RddClosure").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        AtomicInteger counter = new AtomicInteger();
        Integer [] arr={1,2,3,4};

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(arr));

        // Wrong: Don't do this!!
        rdd.foreach(x -> counter.addAndGet(x));

        System.out.println("Counter value: " + counter);
    }
}
