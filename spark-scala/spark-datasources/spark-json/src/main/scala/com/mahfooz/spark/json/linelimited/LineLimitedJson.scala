/*

The line-delimited versus multiline trade-off is controlled by a single option: multiLine.
Line-delimited JSON is actually a much more stable format because it allows you to append to a file with a new record
(rather than having to read in an entire file and then write it out), which is what we recommend that you use.

Another key reason for the popularity of line-delimited JSON is because JSON objects have structure, and JavaScript (on which JSON is based) has at least basic types.

 */
package com.mahfooz.spark.json.linelimited

object LineLimitedJson {

  def main(args: Array[String]): Unit = {

  }

}
