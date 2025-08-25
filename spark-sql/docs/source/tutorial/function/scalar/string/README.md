# String

## Substring function

### instr

```sql
instr(str, substr)
```

Returns the (1-based) index of the first occurrence of substr in str.

```sql
SELECT instr('SparkSQL', 'SQL');
```

## char

```sql
char(expr)
```

- Returns the ASCII character having the binary equivalent to `expr`.
- If `expr` is larger than 256, the result is equivalent to `chr(expr % 256)`.

```sql
SELECT char(65);
```

## char_length

```sql
char_length(expr)
```

- Returns the character length of string data or number of bytes of binary data.
- The length of string data includes the trailing spaces.
- The length of binary data includes binary zeros.

```sql
SELECT char_length('Spark SQL ');
SELECT char_length(x'537061726b2053514c');
SELECT CHAR_LENGTH('Spark SQL ');
SELECT CHARACTER_LENGTH('Spark SQL ');
```

## character_length

```sql
character_length(expr)
```

- Returns the character length of string data or number of bytes of binary data.
- The length of string data includes the trailing spaces.
- The length of binary data includes binary zeros.

Examples:

```sql
SELECT character_length('Spark SQL ');
SELECT character_length(x'537061726b2053514c');
SELECT CHAR_LENGTH('Spark SQL ');
SELECT CHARACTER_LENGTH('Spark SQL ');
```

## chr

```sql
chr(expr)
```

- Returns the ASCII character having the binary equivalent to `expr`.
- If `expr` is larger than 256, the result is equivalent to `chr(expr % 256)`.

Examples:

```sql
SELECT chr(65);
```

## concatenation

### concat

```sql
concat(col1, col2, ..., colN)
```

It returns the concatenation of col1, col2, ..., colN.

```sql
SELECT
      concat('Spark', 'SQL');
SELECT
      concat(array(1, 2, 3), array(4, 5), array(6));  
```

### concat_ws

```sql
concat_ws(sep[, str | array(str)]+)
```

It returns the concatenation of the strings separated by sep, skipping null values.

```sql
SELECT concat_ws(' ', 'Spark', 'SQL');
SELECT concat_ws('s');
SELECT concat_ws('/', 'foo', null, 'bar');
```

## contains

```sql
contains(left, right)
```

- It returns a boolean.
- The value is True if right is found inside left.
- Returns NULL if either input expression is NULL. Otherwise, returns False.
- Both left or right must be of STRING or BINARY type.

```sql
SELECT contains('Spark SQL', 'Spark');
SELECT contains('Spark SQL', 'SPARK');
SELECT contains('Spark SQL', null);
SELECT contains(x'537061726b2053514c', x'537061726b');
```

## conv

```sql
conv(num, from_base, to_base)
```

It converts `num` from `from_base` to `to_base`.

```sql
SELECT conv('100', 2, 10);
SELECT conv(-10, 16, -10);
```

## endswith

```sql
endswith(left, right)
```

- Returns a boolean.
- The value is True if left ends with right.
- Returns NULL if either input expression is NULL. Otherwise, returns False.
- Both left or right must be of STRING or BINARY type.

```sql
SELECT endswith('Spark SQL', 'SQL');
SELECT endswith('Spark SQL', 'Spark');
SELECT endswith('Spark SQL', null);
SELECT endswith(x'537061726b2053514c', x'537061726b');
SELECT endswith(x'537061726b2053514c', x'53514c');
```

## split

```sql
split(str, delimiter)
```

Parameter/Return Type| Data Type|Description
---|---|---
str | `STRING` | The input string to split.
delimiter | `STRING` | The pattern or character to split on (supports regex).
Return type| `ARRAY<STRING>` | Returns an array of strings obtained by splitting the input string.

```sql
-- Basic usage
SELECT split('apple,banana,orange', ',') AS fruits;

-- Accessing specific element
SELECT split('apple,banana,orange', ',')[0] AS first_fruit;

-- Using regex delimiter
SELECT split('cat:dog;fish', '[:;]') AS animals;

-- With a table column
SELECT id,
       split(full_name, ' ')[0] AS first_name,
       split(full_name, ' ')[1] AS last_name
FROM customers;
```

## padding

```sql
SELECT lpad('apple', 10, '*') AS padded_string;
SELECT rpad('apple', 10, '*') AS padded_string;
```
