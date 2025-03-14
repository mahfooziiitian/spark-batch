# Timestamp function

## current_timestamp

    current_timestamp()

Returns the current timestamp at the start of query evaluation. All calls of current_timestamp within the same query return the same value.

    current_timestamp

Returns the current timestamp at the start of query evaluation.

Examples:

    SELECT current_timestamp();
    SELECT current_timestamp;