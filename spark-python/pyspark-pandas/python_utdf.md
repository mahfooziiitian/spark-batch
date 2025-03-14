# Python User-defined Table Functions (UDTFs)

Spark 3.5 introduces the Python user-defined table function (UDTF), a new type of user-defined function.

Unlike scalar functions that return a single result value from each call, each UDTF is invoked in the FROM clause of a query and returns an entire table as output.

Each UDTF call can accept zero or more arguments.

These arguments can either be scalar expressions or table arguments that represent entire input tables.

## Implementing a Python UDTF¶

    class PythonUDTF:

        def __init__(self) -> None
        def eval(self, *args: Any) -> Iterator[Any]
        def terminate(self) -> Iterator[Any]

Initializes the user-defined table function (UDTF). This is optional.

        This method serves as the default constructor and is called once when the
        UDTF is instantiated on the executor side.

        Any class fields assigned in this method will be available for subsequent
        calls to the `eval` and `terminate` methods. This class instance will remain
        alive until all rows in the current partition have been consumed by the `eval`
        method.

        Notes
        -----
        - This method does not accept any extra arguments. Only the default
          constructor is supported.
        - You cannot create or reference the Spark session within the UDTF. Any
          attempt to do so will result in a serialization error.

Evaluates the function using the given input arguments.

        This method is required and must be implemented.

        Argument Mapping:
        - Each provided scalar expression maps to exactly one value in the
          `*args` list.
        - Each provided table argument maps to a pyspark.sql.Row object containing
          the columns in the order they appear in the provided input table,
          and with the names computed by the query analyzer.

        This method is called on every input row, and can produce zero or more
        output rows. Each element in the output tuple corresponds to one column
        specified in the return type of the UDTF.

        Parameters
        ----------
        *args : Any
            Arbitrary positional arguments representing the input to the UDTF.

        Yields
        ------
        tuple
            A tuple representing a single row in the UDTF result table.
            Yield as many times as needed to produce multiple rows.

        Notes
        -----
        - The result of the function must be a tuple representing a single row
          in the UDTF result table.
        - UDTFs currently do not accept keyword arguments during the function call.

        Examples
        --------
        eval that returns one row and one column for each input.

        >>> def eval(self, x: int):
        ...     yield (x, )

        eval that returns two rows and two columns for each input.

        >>> def eval(self, x: int, y: int):
        ...     yield (x + y, x - y)
        ...     yield (y + x, y - x)

Called when the UDTF has processed all input rows.

        This method is optional to implement and is useful for performing any
        cleanup or finalization operations after the UDTF has finished processing
        all rows. It can also be used to yield additional rows if needed.
        Table functions that consume all rows in the entire input partition
        and then compute and return the entire output table can do so from
        this method as well (please be mindful of memory usage when doing
        this).

        Yields
        ------
        tuple
            A tuple representing a single row in the UDTF result table.
            Yield this if you want to return additional rows during termination.

        Examples
        --------
        >>> def terminate(self) -> Iterator[Any]:
        >>>     yield "done", None

## TABLE input argument

Python UDTFs can also take a TABLE as input argument, and it can be used in conjunction with scalar input arguments.

By default, you are allowed to have only one TABLE argument as input, primarily for performance reasons.

If you need to have more than one TABLE input argument, you can enable this by setting the `spark.sql.tvf.allowMultipleTableArguments.enabled`
configuration to true.
