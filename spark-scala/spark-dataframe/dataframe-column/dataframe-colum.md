Since the code refers to data attributes by name, the compiler can’t catch any errors.
If attribute names are incorrect, then the error will only be detected at runtime when the query plan is created.
Another downside with the DataFrame API is that it is very Scala-centric, and while it does support Java, the support is limited. 
For example, when building a DataFrame from an existing RDD of Java objects, Spark’s Catalyst optimizer cannot infer the schema and assumes that any objects in the DataFrame implement the Scala Product interface.
Scala case classes work out the box because they realize this interface.