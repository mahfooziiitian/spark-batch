# Map

The map high-order function in Spark SQL is used to transform each element of an array using a lambda function. It's ideal for applying logic to array elements, such as scaling values, formatting strings, or extracting fields from structs.

## IoT: Normalize Sensor Values

```sql
CREATE OR REPLACE TEMP VIEW iot_stream AS
SELECT * FROM VALUES
  ('dev01', array(25, 105, 98, 5)),
  ('dev02', array(45, 30, 110, 0))
AS iot_stream(device_id, sensor_values);

SELECT 
  device_id,
  map(sensor_values, x -> x / 100.0) AS normalized_values
FROM iot_stream;
```
