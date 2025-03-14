/*

Mode        Description
append      This appends the DataFrame data to the list of files that already exist at the specified destination location.
overwrite   This completely overwrites any data files that already exist at the specified destination location with the data
            in the DataFrame.
error
errorIfExists
default     This is the default mode. If the specified destination location exists, then DataFrameWriter will throw an error.
ignore      If the specified destination location exists, then simply do nothing.
            In other words, silently don’t write out the data in the DataFrame

 */
package com.mahfooz.spark.df.writer.mode

object DataFrameWriterMode {

}
