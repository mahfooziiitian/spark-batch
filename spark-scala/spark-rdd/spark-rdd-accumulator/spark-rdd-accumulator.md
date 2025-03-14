Spark Accumulators are shared variables which are only added

1. through an associative 

    
    A op (B op C) = (A op B) op C
    f(x, y) = f(y, x)

4. commutative operation

    
    A op B = B op A
    f(f(x, y), z) = f(f(x, z), y) = f(f(y, z), x)

3. are used to perform counters (Similar to Map-reduce counters) or sum operations.

**Accumulators can only be written by workers and read by the driver program.**

**They allow us to aggregate values from workers back to the driver.**

**Now only the driver can access the value of the accumulator for the tasks the accumulators are basically write-only.**

**sum and max functions satisfy the above conditions whereas average does not.**


And we can use this as an example to count errors that are seen in an RDD across workers.

Aggregate values from workers back to driver
Only driver can access value of accumulator
For tasks, accumulators are write-only
Use to count errors seen in RDD across workers

Variables that can only be "added" to buy associative operations.

Used to efficiently implement parallel counters and sums
Only driver can read an accumulator’s value, not tasks
Types: integers, double, long, float

Spark by default supports to create an accumulators of any numeric type and provide a capability to add custom accumulator types.
Programmers can create following accumulators

    a) named accumulators
    b) unnamed accumulators

When you create a named accumulator, you can see them on Spark web UI under the "Accumulator" tab.
On this tab, you will see two tables; the first table accumulate – consists of all named 
accumulator variables and their values. 
And on the second table "Tasks" – value for each accumulator modified by a task.
And, unnamed accumulators are not shows on Spark web UI, For all practical purposes it is suggestable to use named accumulators.
