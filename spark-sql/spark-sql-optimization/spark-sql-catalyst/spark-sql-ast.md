# AST

Spark SQL Optimization starts from relation to be computed.

It is computed either from abstract syntax tree (AST) returned by SQL parser or dataframe object created using API.

Both may contain unresolved attribute references or relations.

Spark SQL make use of Catalyst rules and a Catalog object that track data in all data sources to resolve these attributes.

The first step in query optimization is abstracting from the language used to write the query (SQL, Scala, Python,…) to a tree representation.

This procedure starts by considering the query and the datasets: any unresolved attribute found will then pass through the catalog (an entity dictionary) in order to be resolved. Once all attributes will have been resolved, the Abstract Syntax Tree representation is obtained.

In an AST each node represents either data, therefore a RDD, or an operation on one or more RDD. As an example, consider the expression x + (1+2), whose representation can be found in the image below.

A relevant effect of this abstraction is the decoupling between the language used to write the query and the query that the Catalyst will optimize. As a result, the query that will be executed won’t be affected in any measure by the language we have chosen to write it. This, however, holds only in a situation in which we are not making use of User Defined Function. If there is the usage of UDFs the language used becomes relevant performance-wise. As a rule of thumb, scala UDFs will be quicker than Python UDFs, unless Arrow is used. If Arrow is used, python UDFs will be faster.
