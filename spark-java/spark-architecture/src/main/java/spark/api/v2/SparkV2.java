/*

Dataset API and DataFrame API are unified.

In Scala, DataFrame becomes a type alias forDataset[Row], while Java API users must replace DataFrame with Dataset. 

Both the typed transformations (e.g., map, filter, and groupByKey) and untyped transformations 
(e.g.,select and groupBy) are available on the Dataset class.

Since compile-time type-safety in Python and R is not a language feature, the concept of Dataset does not apply to these languages’ APIs.

Instead, DataFrame remains the primary programing abstraction, which is analogous to the single-node data frame notion in these languages.

Dataset and DataFrame API unionAll has been deprecated and replaced by union




 */
package spark.api.v2;

public class SparkV2 {

}
