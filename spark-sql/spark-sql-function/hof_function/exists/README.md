# exists hof

## Checking for Any Even Number

    SELECT exists(array(1, 2, 3, 4, 5), x -> x % 2 = 0) AS has_even;

## Checking for Any Element Greater Than a Certain Value

    SELECT exists(array(1, 2, 3, 4, 5), x -> x > 3) AS has_greater_than_three;

## Checking for Any Null Value

    SELECT exists(array(1, NULL, 3, NULL, 5), x -> x IS NULL) AS has_null;

## Checking for Any Nested Array with a Certain Condition

    SELECT exists(array(array(1, 2), array(), array(3, 4)), a -> exists(a, x -> x = 3)) AS has_nested_with_three;

## Checking for Any String with a Certain Length

    SELECT exists(array('apple', 'cat', 'banana', 'dog'), x -> length(x) > 5) AS has_long_string;

## Checking for Any Struct Element Meeting a Condition

    SELECT exists(
        array(named_struct('name', 'Alice', 'age', 25), named_struct('name', 'Bob', 'age', 30)),
        s -> s.age > 28
    ) AS has_age_greater_than_28;
