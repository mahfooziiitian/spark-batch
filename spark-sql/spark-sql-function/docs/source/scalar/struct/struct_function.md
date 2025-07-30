# Struct function

## named_struct

    named_struct(name1, val1, name2, val2, ...)

It creates a struct with the given field names and values.

    SELECT named_struct("a", 1, "b", 2, "c", 3);
