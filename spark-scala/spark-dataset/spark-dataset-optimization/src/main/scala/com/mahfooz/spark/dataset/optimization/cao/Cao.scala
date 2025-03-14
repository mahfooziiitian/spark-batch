/*
It improves data processing speed by means of more effective usage of L1, L2, L3 CPU caches. Since they are notably faster compared to the main memory, it turned out that a large fraction of the super your time is spent waiting for data to be fetched from the main memory. As part of Project Tungsten, cache-friendly algorithms and data structures are designed so that Spark applications would spend less time waiting to fetch data from memory and more time doing useful work.

Spark offers:

More effective usage of L1/L2/L3 CPU caches: cache aware layout does not require dereferencing two pointers to randomly located records in memory.
Improved efficiency of operators: sort based shuffle, high cardinality aggregations, sort-merge joins, etc.


 */
package com.mahfooz.spark.dataset.optimization.cao

object Cao {

}
