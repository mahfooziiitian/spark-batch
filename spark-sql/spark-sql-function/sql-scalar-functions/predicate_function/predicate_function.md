# predicate_function

| Function	                          | Description                                                                                                                               |
|------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| ! expr	                            | Logical not.                                                                                                                              |
| expr1 < expr2	                     | Returns true if `expr1` is less than `expr2`.                                                                                             |
| expr1 <= expr2	                    | Returns true if `expr1` is less than or equal to `expr2`.                                                                                 |
| expr1 <=> expr2                    | 	Returns same result as the EQUAL(=) operator for non-null operands, but returns true if both are null, false if one of the them is null. |
| expr1 = expr2	                     | Returns true if `expr1` equals `expr2`, or false otherwise.                                                                               |
| expr1 == expr2	                    | Returns true if `expr1` equals `expr2`, or false otherwise.                                                                               |
| expr1 > expr2	                     | Returns true if `expr1` is greater than `expr2`.                                                                                          |
| expr1 >= expr2	                    | Returns true if `expr1` is greater than or equal to `expr2`.                                                                              |
| expr1 and expr2	                   | Logical AND.                                                                                                                              |
| str ilike pattern[ ESCAPE escape]	 | Returns true if str matches `pattern` with `escape` case-insensitively, null if any arguments are null, false otherwise.                  |
| expr1 in(expr2, expr3, ...)	       | Returns true if `expr` equals to any valN.                                                                                                |
| isnan(expr)	                       | Returns true if `expr` is NaN, or false otherwise.                                                                                        |
| isnotnull(expr)	                   | Returns true if `expr` is not null, or false otherwise.                                                                                   |
| isnull(expr)	                      | Returns true if `expr` is null, or false otherwise.                                                                                       |
| str like pattern[ ESCAPE escape]	  | Returns true if str matches `pattern` with `escape`, null if any arguments are null, false otherwise.                                     |
| not expr	                          | Logical not.                                                                                                                              |
| expr1 or expr2	                    | Logical OR.                                                                                                                               |
| regexp(str, regexp)	               | Returns true if `str` matches `regexp`, or false otherwise.                                                                               |
| regexp_like(str, regexp)	          | Returns true if `str` matches `regexp`, or false otherwise.                                                                               |
| rlike(str, regexp)	                | Returns true if `str` matches `regexp`, or false otherwise.                                                                               |
