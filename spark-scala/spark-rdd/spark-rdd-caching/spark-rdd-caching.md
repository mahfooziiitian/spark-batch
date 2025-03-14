# Storage level

1. **Memory only Storage level**

    StorageLevel.MEMORY_ONLY is the default behavior of the RDD method and 
    stores the RDD or DataFrame as deserialized objects to JVM memory. 
    
    When there is not enough memory available it will not save DataFrame of some partitions and these will be re-computed as and when required.

2. **Serialize in Memory**

    StorageLevel.MEMORY_ONLY_SER is the same as MEMORY_ONLY but the difference being it stores RDD as serialized objects to JVM memory.
    
    It takes lesser memory (space-efficient) than MEMORY_ONLY as it saves objects as serialized and takes an additional few more CPU cycles in order to deserialize.

3. **Memory only and Replicate**

   StorageLevel.MEMORY_ONLY_2 is same as MEMORY_ONLY storage level but replicate each partition to two cluster nodes.

4. 


| Storage level                      | Description                                                                                                                                                                                                                                                                                                                                                                     |
|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
 | Serialized in Memory and Replicate | StorageLevel.MEMORY_ONLY_SER_2 is same as MEMORY_ONLY_SER storage level but replicate each partition to two cluster nodes.                                                                                                                                                                                                                                                      |
| Memory and Disk Storage level      | StorageLevel.MEMORY_AND_DISK is the default behavior of the DataFrame or Dataset. In this Storage Level, The DataFrame will be stored in JVM memory as deserialized objects. When required storage is greater than available memory, it stores some of the excess partitions into a disk and reads the data from the disk when required. It is slower as there is I/O involved. |
|Serialize in Memory and Disk|StorageLevel.MEMORY_AND_DISK_SER is same as MEMORY_AND_DISK storage level difference being it serializes the DataFrame objects in memory and on disk when space is not available.|



| Storage Level       | Space used | CPU time | In memory | On-disk | Serialized | Recompute some partitions |
|---------------------|------------|----------|-----------|---------|------------|---------------------------|
| MEMORY_ONLY         | High       | Low      | Y         | N       | N          | Y                         |    
| MEMORY_ONLY_SER     | Low        | High     | Y         | N       | Y          | Y                         |
| MEMORY_AND_DISK     | High       | Medium   | Some      | Some    | Some       | N                         |
| MEMORY_AND_DISK_SER | Low        | High     | Some      | Some    | Y          | N                         |
| DISK_ONLY           | Low        | High     | N         | Y       | Y          | N                         |

# persist

# cache
