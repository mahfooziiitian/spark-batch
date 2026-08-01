# Numeric Precision Issues

Handling large numbers and high-precision decimals without data loss.

## The Risk

```json
{"transaction_id": 9999999999999999999, "amount": 1234567890.123456789}
```

| Type | Problem |
|------|---------|
| `LongType` | Overflows at 9223372036854775807 (2^63 - 1) |
| `DoubleType` | Only ~15-17 significant digits — loses trailing precision |
| `StringType` | Preserves everything — parse later |
| `DecimalType(38,18)` | Exact precision up to 38 digits |

## Safe Schema

```python
from pyspark.sql.types import DecimalType, StringType, StructField, StructType

schema = StructType([
    StructField("transaction_id", StringType(), True),    # NEVER numeric for IDs
    StructField("amount", DecimalType(18, 2), True),      # Exact financial
    StructField("fee", DecimalType(10, 6), True),         # High-precision rate
])
```

!!! success "Golden Rules"
    - **Identifiers** → always `STRING` (even if they look numeric)
    - **Money/financial** → always `DECIMAL(precision, scale)`
    - **Scientific** → `DOUBLE` (when range matters more than precision)
    - **Counters** → `BIGINT` (only if guaranteed < 2^63)

## Double vs Decimal

```python
# Classic floating-point problem
0.1 + 0.2 = 0.30000000000000004  # DOUBLE
0.1 + 0.2 = 0.300000000000000000 # DECIMAL — exact
```

## Inference Options

### `prefersDecimal`

```python
df = spark.read.option("prefersDecimal", "true").json(path)
# Floating-point numbers inferred as DecimalType instead of DoubleType
```

### `primitivesAsString`

```python
df = spark.read.option("primitivesAsString", "true").json(path)
# ALL values become StringType — zero precision loss, cast later
```

## Full Demo

```python title="examples/06_schema/27_numeric_precision.py"
--8<-- "examples/06_schema/27_numeric_precision.py"
```

## Run

```bash
python examples/06_schema/27_numeric_precision.py
```

## Type Decision Guide

| Use Case | Type | Why |
|----------|------|-----|
| IDs, codes, references | `STRING` | Never overflow, exact match |
| Money, prices | `DECIMAL(18,2)` | Exact arithmetic |
| Rates, percentages | `DECIMAL(10,6)` | High precision without rounding |
| Scientific data | `DOUBLE` | Wide range, acceptable precision |
| Row counts, integers < 2^63 | `BIGINT` | Fast integer math |
| Unknown/untrusted numbers | `STRING` | Parse later with explicit casting |

!!! warning "Common Mistake"
    Using `BIGINT` for external IDs (API response IDs, Snowflake IDs, etc.)
    works until the upstream system generates a value > 9223372036854775807.
    Always use `STRING` for identifiers.
