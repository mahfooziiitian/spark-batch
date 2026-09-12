# :material-rotate-3d-variant: Iterative Broadcast (Manual Multi-Pass Join)

Iterative broadcast is a manual technique, not a built-in Spark 4.2 operator. You split a would-be broadcast side into chunks, join each chunk separately, then union the results.

### :material-animation-play: Interactive Visualization — Iterative Broadcast Passes

<div id="viz-joins-skew-iterative-broadcast" class="ts-viz"></div>

The visualization shows the trade-off directly: lower peak broadcast size in exchange for multiple passes over the large side.

<script src="../../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-check-decagram: Verified as a Manual Pattern

In a PySpark 4.2 check, three separate broadcast chunks produced partial counts `[136, 132, 132]`, totaling `400`, which matched the `400` rows from a single broadcast join over the unsplit small side.

That confirms correctness for the toy case. Spark still did **not** show an operator named `IterativeBroadcast`; each pass was just an ordinary join.

______________________________________________________________________

## :material-information-outline: How It Works

1. Partition the smaller side into deterministic chunks.
2. Broadcast one chunk at a time.
3. Join each chunk against the large side.
4. `UNION ALL` the partial outputs.

______________________________________________________________________

## :material-code-tags: Sketch

```sql
-- Conceptual pattern
select * from big_fact f join broadcast_chunk_0 d on f.k = d.k
union all
select * from big_fact f join broadcast_chunk_1 d on f.k = d.k
union all
select * from big_fact f join broadcast_chunk_2 d on f.k = d.k;
```

______________________________________________________________________

## :material-alert-outline: Why It Is Usually a Last Resort

- The large side is scanned multiple times.
- You create more jobs and more unions.
- It adds orchestration complexity that Spark does not manage for you.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

Use iterative broadcast only when a single broadcast would be too large, the alternative shuffle plan is even worse, and you can tolerate multiple passes over the big table.
