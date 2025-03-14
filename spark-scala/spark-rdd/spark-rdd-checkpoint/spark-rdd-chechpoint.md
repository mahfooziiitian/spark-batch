# Checkpointing

It is the act of saving an RDD to disk so that future references to this RDD point to those
intermediate partitions on disk rather than recomputing the RDD from its original source.

This is similar to caching except that it’s not stored in memory, only disk.

