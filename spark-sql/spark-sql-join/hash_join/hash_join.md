# Hash join

In spark, `Hash Join` plays a role at `per node level` and the strategy is used to `join partitions` available on the node.

1. Create `hash table` based `join key` of `small table`
2. `Loop` over `large table` and matched the `hashed join key`.

Hash Join is performed by first creating a Hash Table based on `join_key` of smaller relation
and then looping over larger relation to match the `hashed join_key` values.

**Also, this is only supported for '=' join.**
