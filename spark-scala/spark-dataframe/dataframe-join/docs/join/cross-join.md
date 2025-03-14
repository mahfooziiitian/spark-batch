# Cross join

Cross-joins in the simplest terms are inner joins that do not specify a predicate.
Cross joins will join every single row in the left DataFrame to ever single row in the right DataFrame.
This will cause an absolute explosion in the number of rows contained in the resulting DataFrame.
If you have 1,000 rows in each DataFrame, the cross-join of these will result in 1,000,000 (1,000 x 1,000) rows.
For this reason, you must very explicitly state that you want a cross-join by using the cross-join keyword.

## Null consideration