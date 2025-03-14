"""
The numPartitions parameter specifies the number of partitions to create.

The range between lowerBound and upperBound is divided into equal-sized partitions.

Each partition covers a stride equal to:

    (upperBound - lowerBound) / numPartitions

"""