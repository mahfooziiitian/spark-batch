# nulls_option

Specifies whether to skip null values when evaluating the window function.

`RESPECT NULLS` means not skipping null values, while `IGNORE NULLS` means skipping.

If not specified, the `default` is `RESPECT NULLS`.

## Syntax:

    { IGNORE | RESPECT } NULLS

Note: `Only LAG | LEAD | NTH_VALUE | FIRST_VALUE | LAST_VALUE can be used with IGNORE NULLS`.
