# explode array

    explode(expr)

Separates the elements of array expr into multiple rows, or the elements of map expr into multiple rows and columns.

Unless specified otherwise, uses the default column name col for elements of the array or key and value for the elements of the map.

    SELECT explode(array(10, 20));
    SELECT explode(collection => array(10, 20));
    SELECT * FROM explode(collection => array(10, 20));

## explode_outer

    explode_outer(expr)

Separates the elements of array expr into multiple rows, or the elements of map expr into multiple rows and columns.

Unless specified otherwise, uses the default column name col for elements of the array or key and value for the elements of the map.

    SELECT explode_outer(array(10, 20));
    SELECT explode_outer(collection => array(10, 20));
    SELECT * FROM explode_outer(collection => array(10, 20));

## inline_outer

    inline_outer(expr)

Explodes an array of structs into a table.

Uses column names col1, col2, etc by default unless specified otherwise.

    SELECT inline_outer(array(struct(1, 'a'), struct(2, 'b')));

## inline

    inline(expr)

Explodes an array of structs into a table.

Uses column names col1, col2, etc. by default unless specified otherwise.

Examples:

    SELECT inline(array(struct(1, 'a'), struct(2, 'b')));
