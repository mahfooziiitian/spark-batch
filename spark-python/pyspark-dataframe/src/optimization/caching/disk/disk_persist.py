"""
RDDs, whose partitions are too large to be stored in RAM on each of the
executors, can be written to disk. This strategy is obviously slower for
repeated computations, but can be more fault-tolerant for long sequences of
transformations, and may be the only feasible option for enormous computations.

"""