"""
Delivering end-to-end exactly-once semantics was one of key goals behind the design of Structured Streaming.

To achieve that, we have designed the Structured Streaming sources, the sinks and the execution engine to
reliably track the exact progress of the processing so that it can handle any kind of failure by restarting and/or
reprocessing.

Every streaming source is assumed to have offsets (similar to Kafka offsets, or Kinesis sequence numbers) to
track the read position in the stream.

The engine uses checkpointing and write-ahead logs to record the offset range of the data being processed in each trigger

The streaming sinks are designed to be idempotent for handling reprocessing.

Together, using re-playable sources and idempotent sinks, Structured Streaming can ensure end-to-end exactly-once
semantic under any failure.
"""