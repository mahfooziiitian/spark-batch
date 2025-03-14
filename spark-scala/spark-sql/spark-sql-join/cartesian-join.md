# Cartesian join

This join type is the simplest to use because the join expression is not needed.
Its behavior can be a bit dangerous because it joins every single row in the left dataset with every row in the right dataset.

The size of the joined dataset is the product of the size of the two datasets.

For example, if the size of each dataset is 1,024, then the size of the joined dataset is more than 1 million rows.

For this reason, the way to use this join type is by explicitly using a dedicated transformation in DataFrame, rather than specifying this
join type as a string.

## Broadcast nested loop join

Think of this as a nested loop comparison of both the relations:

    for record_1 in relation_1:
        for record_2 in relation_2:
            # join condition is executed

As you can see, this can be a very slow strategy.
This is generally, a fallback option when no other join type can be applied.

## Note

1. Supports both '=' and non-equi-joins ('≤=', '<' etc.).
2. Supports all the join types
