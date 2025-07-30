# String function

## Substring function

### instr

    instr(str, substr)

Returns the (1-based) index of the first occurrence of substr in str.

    SELECT instr('SparkSQL', 'SQL');

## char

    char(expr)

It returns the ASCII character having the binary equivalent to expr.
If n is larger than 256 the result is equivalent to `chr(n % 256)`

    SELECT char(65);

## char_length

    char_length(expr)

It returns the character length of string data or number of bytes of binary data.
The length of string data includes the trailing spaces.
The length of binary data includes binary zeros.

    SELECT char_length('Spark SQL ');
    SELECT char_length(x'537061726b2053514c');
    SELECT CHAR_LENGTH('Spark SQL ');
    SELECT CHARACTER_LENGTH('Spark SQL ');

## character_length

    character_length(expr)

It returns the character length of string data or number of bytes of binary data.
The length of string data includes the trailing spaces.
The length of binary data includes binary zeros.

Examples:

    SELECT character_length('Spark SQL ');
    SELECT character_length(x'537061726b2053514c');
    SELECT CHAR_LENGTH('Spark SQL ');
    SELECT CHARACTER_LENGTH('Spark SQL ');

## chr

    chr(expr)

Returns the ASCII character having the binary equivalent to expr.
If n is larger than 256 the result is equivalent to `chr(n % 256)`

Examples:

    SELECT chr(65);

## concatenation

### concat

    concat(col1, col2, ..., colN)

It returns the concatenation of col1, col2, ..., colN.

    SELECT
      concat('Spark', 'SQL');
    SELECT
      concat(array(1, 2, 3), array(4, 5), array(6));  

### concat_ws

    concat_ws(sep[, str | array(str)]+)

It returns the concatenation of the strings separated by sep, skipping null values.

    SELECT concat_ws(' ', 'Spark', 'SQL');
    SELECT concat_ws('s');
    SELECT concat_ws('/', 'foo', null, 'bar');
    SELECT concat_ws(null, 'Spark', 'SQL');

## contains

    contains(left, right)

It returns a boolean.
The value is True if right is found inside left.
Returns NULL if either input expression is NULL.
Otherwise, returns False.
Both left or right must be of STRING or BINARY type.

    SELECT contains('Spark SQL', 'Spark');
    SELECT contains('Spark SQL', 'SPARK');
    SELECT contains('Spark SQL', null);
    SELECT contains(x'537061726b2053514c', x'537061726b');

## conv

    conv(num, from_base, to_base)

It converts `num` from `from_base` to `to_base`.

    SELECT conv('100', 2, 10);
    SELECT conv(-10, 16, -10);

## endswith

    endswith(left, right)

Returns a boolean.
The value is True if left ends with right.
Returns NULL if either input expression is NULL.
Otherwise, returns False.
Both left or right must be of STRING or BINARY type.

    SELECT endswith('Spark SQL', 'SQL');
    SELECT endswith('Spark SQL', 'Spark');
    SELECT endswith('Spark SQL', null);
    SELECT endswith(x'537061726b2053514c', x'537061726b');
    SELECT endswith(x'537061726b2053514c', x'53514c');

## padding
