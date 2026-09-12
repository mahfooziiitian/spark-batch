# :material-call-split: Separating Hot Keys from Normal Keys

A practical manual skew strategy is to send the worst hot keys through a special join path and keep the rest of the data on a normal join path.

### :material-animation-play: Interactive Visualization — Split Heavy Keys, Then Recombine

<div id="viz-joins-skew-separation" class="ts-viz"></div>

This split-path view is useful when only a handful of values cause the skew and you want a targeted fix instead of salting every row.

<script src="../../../../assets/js/querying-joins-strategy-viz.js"></script>

______________________________________________________________________

## :material-information-outline: Why It Works

If only a few keys are pathological, you do not need to penalize the entire dataset. Instead:

1. Identify hot keys.
2. Route hot keys into a specialized path.
3. Keep normal keys on a regular equi-join.
4. `UNION ALL` the results.

______________________________________________________________________

## :material-code-tags: Example Pattern

```sql
with hot_keys as (
    select customer_id
    from fact_orders
    group by customer_id
    having count(*) > 100000
),
hot_fact as (
    select f.*
    from fact_orders f
    join hot_keys h
      on f.customer_id = h.customer_id
),
normal_fact as (
    select f.*
    from fact_orders f
    left anti join hot_keys h
      on f.customer_id = h.customer_id
)
select /*+ broadcast(dim_customer) */ *
from hot_fact hf
join dim_customer dc
  on hf.customer_id = dc.customer_id
union all
select *
from normal_fact nf
join dim_customer dc
  on nf.customer_id = dc.customer_id;
```

______________________________________________________________________

## :material-table: When It Beats Salting

| Better for separation                  | Better for salting                                          |
| -------------------------------------- | ----------------------------------------------------------- |
| Only a few known hot keys              | Many skewed keys or unknown heavy values                    |
| You want different strategies per path | You want one uniform transformed join                       |
| You can maintain a hot-key list        | You prefer automatic distribution once keys are transformed |

______________________________________________________________________

## :material-alert-outline: Costs

- More query text and more maintenance.
- Need a trustworthy way to identify hot keys.
- Potential double work if the split predicates are not mutually exclusive.

______________________________________________________________________

## :material-lightbulb-outline: When to Use

Use manual separation when skew is highly concentrated and identifiable, especially if the hot path can use broadcast while the normal path keeps a standard shuffle join.
