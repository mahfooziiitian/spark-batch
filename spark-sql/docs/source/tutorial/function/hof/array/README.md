# Introduction

## zip_with

```sql
zip_with(array<T1>, array<T2>, function<T1, T2, R>) → array<R>
```

```sql
SELECT zip_with(array(1,2,3), array(4,5,6), (x,y) -> x + y) AS summed;
```

## forall

```sql
forall(array<T>, function<T, Boolean>) → Boolean
```

```sql
SELECT forall(array(1,2,3), (x) -> x > 0) AS all_positive;
```

## exists

```sql
exists(array<T>, function<T, Boolean>) → Boolean
```

## filter

```sql
filter(array<T>, function<T, Boolean>) → array<T>
```

```sql
SELECT filter(array(1,2,3), (x) -> x > 1) AS greater_than_one;
```

## transform

```sql
transform(array<T>, function<T, U>) → array<U>
```

```sql
SELECT transform(array(1,2,3), (x) -> x * 2) AS doubled;
```

```sql
-- [2, 4, 6]