/*



def schema(schemaString: String): DataFrameReader
  Specifies the schema by using the input DDL-formatted string.

def schema(schema: StructType): DataFrameReader
  Specifies the input schema.

Some data sources (e.g. JSON) can infer the input schema automatically from data.
By specifying the schema here, the underlying data source can skip the schema inference step, and thus speed up data loading.

 */
package com.mahfooz.spark.ds.dfreader.schema

object DFReaderSchema {}
