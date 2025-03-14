/*

As mentioned, this column might or might not exist in our DataFrames.
Columns are not resolved until we compare the column names with those we are maintaining in the catalog.
Column and table resolution happens in the analyzer phase.
Scala has some unique language features that allow for more shorthand ways of referring to columns.

The $ allows us to designate a string as a special string that should refer to an expression.
The tick mark (') is a special thing called a symbol; this is a Scala-specific construct of referring to some identifier.
They both perform the same thing and are shorthand ways of referring to columns by name.
 */
package com.mahfooz.dataframe.columns.resolution

object ColumnResolution {

}
