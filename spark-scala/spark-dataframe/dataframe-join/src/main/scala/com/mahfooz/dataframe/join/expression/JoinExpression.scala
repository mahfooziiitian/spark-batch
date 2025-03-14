/*

The choice of expression for performing join operations in Spark SQL depends on various factors such as:

Data size and complexity: If the data size and complexity are high, a column object expression may be preferred as it provides a clean and readable syntax while being efficient and well supported by Spark SQL.

Performance: Performance is a critical factor in real-time scenarios, and the choice of expression should be based on the query’s performance requirements. In general, column object expressions are preferred for join operations, as they are efficient and well supported by Spark SQL.

Readability: The readability of the code is also an important factor in real-time scenarios, as it affects the maintainability and debug-ability of the code. Column object expressions are recommended for join operations, as they provide a clean and readable syntax.

Flexibility: In some cases, the join condition may require complex operations or calculations. In such cases, Spark SQL expressions may be preferred as they allow you to perform complex operations using Spark SQL functions.

Customization: String expressions are preferred in scenarios where customization is required, as they allow you to write join conditions as plain text.


 */
package com.mahfooz.dataframe.join.expression

object JoinExpression {

  //ON boolean_expression

  // USING ( column_name [ , ... ] )

  //Join Type


}
