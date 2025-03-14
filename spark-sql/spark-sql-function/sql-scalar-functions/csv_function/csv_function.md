# Csv function

## from_csv

    from_csv(csvStr, schema[, options])

Returns a struct value with the given csvStr and schema.

    SELECT from_csv('1, 0.8', 'a INT, b DOUBLE');
    SELECT from_csv('26/08/2015', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
