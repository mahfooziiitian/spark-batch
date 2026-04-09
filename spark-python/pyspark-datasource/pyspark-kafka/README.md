# pyspark kafka

## Create kafka topic
    
    kafka-topics \
        --create \
        --topic movies \
        --bootstrap-server broker-2:29092

    kafka-topics.sh \
        --create \
        --topic movies \
        --bootstrap-server localhost:19091

## Writing some data to topic

    kafka-console-producer \
        --topic events \
        --bootstrap-server localhost:29092
This is my first event
This is my second event

## References

1. <https://www.waitingforcode.com/apache-spark-structured-streaming/corrupted-records-poison-pill-records-apache-spark-structured-streaming/read>
2. 
 