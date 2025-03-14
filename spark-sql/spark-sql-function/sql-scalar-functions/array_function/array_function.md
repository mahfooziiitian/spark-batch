# Array function

## array

Returns an array with the given elements.

    array(expr, ...)

### Ex

    SELECT array(1, 2, 3);

## array_agg

It collects and returns a list of non-unique elements.

    SELECT array_agg(col) FROM VALUES (1), (2), (1) AS tab(col);

## array_append

    SELECT array_append(array(1, 2, 3, null), null);
    
    SELECT array_append(CAST(null as array<Int>), 2);

## array_compact

    array_compact(array) - Removes null values from the array.

    SELECT array_compact(array(1, 2, 3, null));
    SELECT array_compact(array("a", "b", "c"));

## array_contains

    array_contains(array, value) - 

It returns true if the array contains the value.

## array_distinct

    array_distinct(array)

It removes duplicate values from the array.

`Examples:`

    SELECT array_distinct(array(1, 2, 3, null, 3));

## array_except

    array_except(array1, array2)
It returns an array of the elements in array1 but not in array2, without duplicates.

`Examples:`

    SELECT array_except(array(1, 2, 3), array(1, 3, 5));

## array_insert

    array_insert(x, pos, val)

It places val into index pos of array x. Array indices start at 1.
The maximum negative index is -1 for which the function inserts new element after the current last
element. Index above array size appends the array, or prepends the array if index is negative,
with 'null' elements.

Examples:

    SELECT array_insert(array(1, 2, 3, 4), 5, 5);
    SELECT array_insert(array(5, 4, 3, 2), -1, 1);
    SELECT array_insert(array(5, 3, 2, 1), -4, 4);

## array_intersect

array_intersect(array1, array2) -

Returns an array of the elements in the intersection of array1 and array2, without duplicates.

    SELECT array_intersect(array(1, 2, 3), array(1, 3, 5));

## array_join

    array_join(array, delimiter[, nullReplacement])

Concatenates the elements of the given array using the delimiter and an optional string to replace nulls.
If no value is set for nullReplacement, any null value is filtered.

    SELECT array_join(array('hello', 'world'), ' ');
    SELECT array_join(array('hello', null ,'world'), ' ');
    SELECT array_join(array('hello', null ,'world'), ' ', ',');

## array_max

    array_max(array)

It returns the maximum value in the array.

NaN is greater than any non-NaN elements for double/float type.

NULL elements are skipped.

Examples:

    SELECT array_max(array(1, 20, null, 3));

## array_min

    array_min(array)

It returns the minimum value in the array.

NaN is greater than any non-NaN elements for double/float type.

NULL elements are skipped.

    SELECT array_min(array(1, 20, null, 3));

## array_position

    array_position(array, element)

It returns the (1-based) index of the first matching element of the array as long, or 0 if no match is found.

    SELECT array_position(array(312, 773, 708, 708), 708);
    SELECT array_position(array(312, 773, 708, 708), 414);

## array_prepend

    array_prepend(array, element)

Add the element at the beginning of the array passed as first argument.
Type of element should be the same as the type of the elements of the array.
Null element is also prepended to the array.
But if the array passed is NULL output is NULL

Examples:

    SELECT array_prepend(array('b', 'd', 'c', 'a'), 'd');
    SELECT array_prepend(array(1, 2, 3, null), null);
    SELECT array_prepend(CAST(null as Array<Int>), 2);

## array_remove

    array_remove(array, element)

Remove all elements that equal to element from array.

## array_repeat

    array_repeat(element, count)

Returns the array containing element count times.

Examples:

    SELECT array_repeat('123', 2);

## array_size

    array_size(expr)

Returns the size of an array.

The function returns null for null input.

Examples:

    SELECT array_size(array('b', 'd', 'c', 'a'));

## array_sort

    array_sort(expr, func)

Sorts the input array. If func is omitted, sort in ascending order.

The elements of the input array must be orderable. 

NaN is greater than any non-NaN elements for double/float type.

Null elements will be placed at the end of the returned array.

Since 3.0.0 this function also sorts and returns the array based on the given comparator function.

The comparator will take two arguments representing two elements of the array. It returns a negative integer, 0, or a positive integer as the first element is less than, equal to, or greater than the second element.

If the comparator function returns null, the function will fail and raise an error.

    SELECT array_sort(array(5, 6, 1), (left, right) -> case when left < right then -1 when left > right then 1 else 0 end);
    SELECT array_sort(array('bc', 'ab', 'dc'), (left, right) -> case when left is null and right is null then 0 when left is null then -1 when right is null then 1 when left < right then 1 when left > right then -1 else 0 end);
    SELECT array_sort(array('b', 'd', null, 'c', 'a'));

## array_union

    array_union(array1, array2)

It returns an array of the elements in the union of array1 and array2, without duplicates.

    SELECT array_union(array(1, 2, 3), array(1, 3, 5));

## arrays_overlap

    arrays_overlap(a1, a2)

It returns true if a1 contains at least a non-null element present also in a2. If the arrays have no common element and they are both non-empty and either of them contains a null element null is returned, false otherwise.

    SELECT arrays_overlap(array(1, 2, 3), array(3, 4, 5));

## arrays_zip

    arrays_zip(a1, a2, ...)

It returns a merged array of structs in which the N-th struct contains all N-th values of input arrays.

    SELECT arrays_zip(array(1, 2, 3), array(2, 3, 4));
    SELECT arrays_zip(array(1, 2), array(2, 3), array(3, 4));

## element_at

    SELECT element_at(array(1, 2, 3), 2);

## elt

    elt(n, input1, input2, ...)

Returns the n-th input, e.g., returns input2 when n is 2.

The function returns NULL if the index exceeds the length of the array and `spark.sql.ansi.enabled` is set to false.
If `spark.sql.ansi.enabled` is set to true, it throws `ArrayIndexOutOfBoundsException` for invalid indices.

    SELECT elt(1, 'scala', 'java');
    SELECT elt(2, 'a', 1);

## filter

    filter(expr, func)

Filters the input array using the given predicate.

    SELECT filter(array(1, 2, 3), x -> x % 2 == 1);
    SELECT filter(array(0, 2, 3), (x, i) -> x > i);
    SELECT filter(array(0, null, 2, 3, null), x -> x IS NOT NULL);

The inner function may use the index argument since 3.0.0.

## flatten

    flatten(arrayOfArrays)

Transforms an array of arrays into a single array.

    SELECT flatten(array(array(1, 2), array(3, 4)));

## forall

    forall(expr, pred)

Tests whether a predicate holds for all elements in the array.

    SELECT forall(array(1, 2, 3), x -> x % 2 == 0);
    SELECT forall(array(2, 4, 8), x -> x % 2 == 0);
    SELECT forall(array(1, null, 3), x -> x % 2 == 0);
    SELECT forall(array(2, null, 8), x -> x % 2 == 0);

## get

    get(array, index)

Returns element of array at given (0-based) index.
If the index points outside the array boundaries, then this function returns NULL.

    SELECT get(array(1, 2, 3), 0);
    SELECT get(array(1, 2, 3), 3);
    SELECT get(array(1, 2, 3), -1);
