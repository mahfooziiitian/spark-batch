# Spark Reading mode

Reading data from an external source naturally entails encountering malformed data, especially
when working with only semi-structured data sources.

Read modes specify what will happen when Spark does come across malformed records.

## Spark's read modes

| Read mode                                 | Description                                                                                    |
|-------------------------------------------|------------------------------------------------------------------------------------------------|
| permissive                                | Sets all fields to null when it encounters a corrupted record and places all corrupted records |
| in a string column called _corrupt_record |                                                                                                |
| dropMalformed                             | Drops the row that contains malformed records                                                  |
| failFast                                  | Fails immediately upon encountering malformed records                                          |
