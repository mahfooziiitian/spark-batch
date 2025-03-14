# Generator Functions
| Function	                    | Description                                                                                                                                                                                                                                                                                               |
|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| explode(expr)                | 	Separates the elements of array `expr` into multiple rows, or the elements of map `expr` into multiple rows and columns. Unless specified otherwise, uses the default column name `col` for elements of the array or `key` and `value` for the elements of the map.                                      |
| explode_outer(expr)	         | Separates the elements of array `expr` into multiple rows, or the elements of map `expr` into multiple rows and columns. Unless specified otherwise, uses the default column name `col` for elements of the array or `key` and `value` for the elements of the map.                                       |
| inline(expr)	                | Explodes an array of structs into a table. Uses column names col1, col2, etc. by default unless specified otherwise.                                                                                                                                                                                      |
| inline_outer(expr)           | 	Explodes an array of structs into a table. Uses column names col1, col2, etc. by default unless specified otherwise.                                                                                                                                                                                     |
| posexplode(expr)	            | Separates the elements of array `expr` into multiple rows with positions, or the elements of map `expr` into multiple rows and columns with positions. Unless specified otherwise, uses the column name `pos` for position, `col` for elements of the array or `key` and `value` for elements of the map. |
| posexplode_outer(expr)	      | Separates the elements of array `expr` into multiple rows with positions, or the elements of map `expr` into multiple rows and columns with positions. Unless specified otherwise, uses the column name `pos` for position, `col` for elements of the array or `key` and `value` for elements of the map. |
| stack(n, expr1, ..., exprk)	 | Separates `expr1`, ..., `exprk` into `n` rows. Uses column names col0, col1, etc. by default unless specified otherwise.                                                                                                                                                                                  |

## Setup data

    CREATE OR REPLACE TEMPORARY VIEW complex_type 
    USING json OPTIONS (
        path 'file:/mnt/d/Data/FileData/Json/complex_type_null_array.json',
        multiline true
    );

    CREATE OR REPLACE TEMPORARY VIEW complex_array_struct 
    USING json OPTIONS (
        path 'file:/mnt/d/Data/FileData/Json/complex_json_array_struct.json',
        multiline true
    );

## explode

Explode creates a row for each element in the array or map column by ignoring null or empty values in array or map.

    SELECT explode(array(10, 20));
    SELECT explode(collection => array(10, 20));
    SELECT explode(map('a', 20, 'b', 40));
    select explode(info.attributes), * from complex_type

## explode_outer
    SELECT explode(['baseball', 'soccer']);
    SELECT explode(array(null,1,2));
    SELECT explode(map('a', 20, 'b', 40));

    select explode_outer(info.attributes), * from complex_type

## inline
    
    SELECT inline(array(struct(1, 'a'), struct(2, 'b')));
    SELECT  * from complex_array_struct;
    SELECT inline(team), * from complex_array_struct;

## inline_outer

Explodes an array of structs into a table.

    SELECT inline_outer(array(struct(1, 'a'), struct(2, 'b')));
    SELECT inline_outer(team), * from complex_array_struct;

## posexplode
    SELECT  * from complex_array_struct;
    SELECT posexplode(team), * from complex_array_struct;

## posexplode_outer
    SELECT  * from complex_array_struct;
    SELECT posexplode_outer(team), * from complex_array_struct;

## stack

stack(n, expr1, ..., exprk)

Separates expr1, ..., exprk into n rows.

    SELECT stack(2, 1, 2, 3);
