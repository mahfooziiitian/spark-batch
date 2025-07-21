# Conversion Functions

| Function           | Description                                                 |
|--------------------|-------------------------------------------------------------|
| bigint(expr)       |  Casts the value `expr` to the target data type `bigint`.   |
| binary(expr)       |  Casts the value `expr` to the target data type `binary`.   |
| boolean(expr)      |  Casts the value `expr` to the target data type `boolean`.  |
| cast(expr AS type) |  Casts the value `expr` to the target data type `type`.     |
| date(expr)         |  Casts the value `expr` to the target data type `date`.     |
| decimal(expr)      |  Casts the value `expr` to the target data type `decimal`.  |
| double(expr)       |  Casts the value `expr` to the target data type `double`.   |
| float(expr)        | Casts the value `expr` to the target data type `float`.     |
| int(expr)          | Casts the value `expr` to the target data type `int`.       |
| smallint(expr)     |  Casts the value `expr` to the target data type `smallint`. |
| string(expr)       | Casts the value `expr` to the target data type `string`.    |
| timestamp(expr)    | Casts the value `expr` to the target data type `timestamp`. |
| tinyint(expr)      | Casts the value `expr` to the target data type `tinyint`.   |

## Expression to bigint

```sql
SELECT bigint('12345')
```

## Expression to binary

```sql
SELECT hex(binary('abc'))
```

## Expression to boolean

```sql
SELECT cast('2025-01-01' AS date)
```

## Converts to DATE

```sql
SELECT date('2025-07-20')
```

## Converts to DECIMAL(10, 0) unless specified

```sql
SELECT decimal('123.456')
```

## Converts to FLOAT (float32)

```sql
SELECT float('123.456')
```

## Converts to INT (32-bit integer)

```sql
SELECT int('42.99')
```

## Converts to SMALLINT (16-bit integer)

```sql
SELECT smallint('12')
```

## Converts to STRING

```sql
SELECT string(2025)
```

## Converts to TIMESTAMP

```sql
SELECT timestamp('2024-01-01 12:34:56')	2024-01-01 12:34:56
```

## Converts to TINYINT (8-bit integer)

```sql
SELECT tinyint('127')
```

## cast and try_cast()

### cast

Invalid casts (e.g., 'abc' to int) will throw errors.

```sql
SELECT cast('2025-01-01' AS date)
```

### try_cast

Use try_cast() (if available in your Spark version) to avoid failure on bad input.

```sql

```

## 🔍 Bonus: Chained Casting

```sql
SELECT
  CAST(CAST('123.45' AS double) AS int) AS double_to_int;
```
