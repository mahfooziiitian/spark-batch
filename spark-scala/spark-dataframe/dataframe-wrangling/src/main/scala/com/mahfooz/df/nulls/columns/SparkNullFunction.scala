/*
SELECT
  ifnull(null, 'return_value'),
  nullif('value', 'value'),
  nvl(null, 'return_value'),
  nvl2('not_null', 'return_value', "else_value")
FROM dfTable LIMIT 1

ifnull allows you to select the second value if the first is null, and defaults to the first.
nullif, which returns null if the two values are equal or else returns the second if they are not.
nvl returns the second value if the first is null, but defaults to the first.
nvl2 returns the second value if the first is not null; otherwise, it will return the last specified value.

 */
package com.mahfooz.df.nulls.columns

object SparkNullFunction {

}
