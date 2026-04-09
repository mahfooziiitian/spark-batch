# Processing model

The key idea in Structured Streaming is to treat a live data stream as a table that is being continuously appended.

This leads to a new stream processing model that is very similar to a batch processing model.

You will express your streaming computation as standard batch-like query as on a static table, and 
Spark runs it as an incremental query on the unbounded input table.

## Data stream

## Un-bounded table

## Result table

A query on the input will generate the "Result Table".

Every trigger interval (say, every 1 second), new rows get appended to the Input Table, which eventually updates the Result Table.

Whenever the result table gets updated, we would want to write the changed result rows to an external sink.

## Output mode

The "Output" is defined as what gets written out to the external storage.

The output can be defined in a different mode:

### 1. Complete Mode

The entire updated Result Table will be written to the external storage.

It is up to the storage connector to decide how to handle writing of the entire table.

### 2. Append Mode

Only the new rows appended in the Result Table since the last trigger will be written to the external storage.

This is applicable only on the queries where existing rows in the Result Table are not expected to change.

### 3. Update Mode

Only the rows that were updated in the Result Table since the last trigger will be written to the external storage 
(available since Spark 2.1.1).

Note that this is different from the Complete Mode in that this mode only outputs the rows that have changed since the last trigger.

If the query doesn't contain aggregations, it will be equivalent to Append mode.
