# Pool Configuration

Named pools let you partition cluster resources between workload classes.
Pools are defined in an XML file referenced by `spark.scheduler.allocation.file`.

## File Location

```
src/parallel/scheduling/fairscheduler.xml
```

## Current Configuration

```xml title="src/parallel/scheduling/fairscheduler.xml"
--8<-- "src/parallel/scheduling/fairscheduler.xml"
```

## Pool Fields

| Field | Type | Description |
| ----- | ---- | ----------- |
| `schedulingMode` | `FAIR` \| `FIFO` | How tasks within the pool are ordered |
| `weight` | integer | Relative share of resources. Pool A (weight=2) gets twice the slots as Pool B (weight=1) when both are active |
| `minShare` | integer | Minimum number of executor slots guaranteed to this pool even under heavy contention |

## Pool Definitions

### `production` Pool

```xml
<pool name="production">
  <schedulingMode>FAIR</schedulingMode>
  <weight>2</weight>
  <minShare>3</minShare>
</pool>
```

- **FAIR** within the pool: multiple jobs submitted to `production` share its
  slots evenly — no single job starves others.
- **weight=2**: receives twice the executor slots of the `test` pool when both
  have pending tasks.
- **minShare=3**: always gets at least 3 executor slots, even when the `test`
  pool is heavily loaded.

### `test` Pool

```xml
<pool name="test">
  <schedulingMode>FIFO</schedulingMode>
  <weight>1</weight>
  <minShare>1</minShare>
</pool>
```

- **FIFO** within the pool: test/batch jobs run one after another inside the
  pool, which is fine since they don't compete for interactive response time.
- **weight=1**: yields to `production` under contention.
- **minShare=1**: guaranteed at least 1 slot so test jobs are never completely blocked.

## Adding a New Pool

1. Add an entry to `fairscheduler.xml`:
    ```xml
    <pool name="etl">
      <schedulingMode>FAIR</schedulingMode>
      <weight>3</weight>
      <minShare>2</minShare>
    </pool>
    ```
2. Assign threads to it:
    ```python
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "etl")
    ```

## Default Pool

If no pool is set (or an unknown name is used), Spark falls back to the
**default pool**, which uses FIFO mode with weight=1 and minShare=0.

!!! tip "Verify pool assignment"
    Open the Spark Web UI (`http://localhost:4040`) → **Stages** tab.
    Each stage shows its pool name in the description column.

## Pool Weight Decision Guide

| Workload class | Recommended weight |
| -------------- | ------------------ |
| Interactive queries (BI tools, notebooks) | 3–4 |
| Production ETL pipelines | 2 |
| Background batch jobs | 1 |
| CI / test jobs | 1 |
