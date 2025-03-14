/*

You can control bloom filters and dictionary encodings for ORC data sources.
The following ORC example will create bloom filter and use dictionary encoding only for favorite_color.
 */
package com.mahfooz.spark.orc.filters

object OrcBloomFilter {

  def main(args: Array[String]): Unit = {
    """
      |CREATE TABLE users_with_options (
      |  name STRING,
      |  favorite_color STRING,
      |  favorite_numbers array<integer>
      |)
      |USING ORC
      |OPTIONS (
      |  orc.bloom.filter.columns 'favorite_color',
      |  orc.dictionary.key.threshold '1.0',
      |  orc.column.encoding.direct 'name'
      |)
      |""".stripMargin
  }

}
