DataFrames and Datasets are (distributed) table-like collections with well-defined rows and columns.
ach column must have the same number of rows as all the other columns (although you can use null to specify the absence of a value) 
and each column has type information that must be consistent for every row in the collection.

To Spark, DataFrames and Datasets represent immutable, lazily evaluated plans that specify what operations to apply to data residing at a location to generate some output. 

When we perform an action on a DataFrame, we instruct Spark to perform the actual transformations and return the result. 
These represent plans of how to manipulate rows and columns to compute the user’s desired result.
Tables and views are basically the same thing as DataFrames. 

# When to Use Datasets

1. When the operation(s) you would like to perform cannot be expressed using DataFrame
   manipulations 
2. When you want or need type-safety, and you’re willing to accept the cost of
   performance to achieve

# Type safety

Operations that are not valid for their types, say subtracting two
string types, will fail at compilation time not at runtime

