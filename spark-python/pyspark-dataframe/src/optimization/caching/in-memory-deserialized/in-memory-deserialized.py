"""
The most intuitive way to store objects in RDDs is as the original deserialized Java objects that are defined by the
driver program.
This storage is the fastest, since it reduces serialization time; however, it may not be the most memory efficient,
since it requires the data to be stored as objects.

The persist() function in the RDD class lets the user control how the RDD is
stored. By default, persist() stores an RDD as deserialized objects in memory, but
the user can pass one of numerous storage options to the persist() function to con‐
trol how the RDD is stored.

When persisting RDDs, the default implementation of RDDs evicts the least recently used partition (called LRU caching)
if the space it takes is required to compute or to cache a new partition.

However, you can change this behavior and control Spark’s memory prioritization with the persistencePriority() function
in the RDD class.
"""
if __name__ == '__main__':
    print()
