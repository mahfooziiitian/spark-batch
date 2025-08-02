# Json function

## from_json

    from_json(jsonStr, schema[, options])

Returns a struct value with the given `jsonStr` and `schema`.

    SELECT from_json('{"a":1, "b":0.8}', 'a INT, b DOUBLE');
    SELECT from_json('{"time":"26/08/2015"}', 'time Timestamp', map('timestampFormat', 'dd/MM/yyyy'));
    SELECT from_json('{"teacher": "Alice", "student": [{"name": "Bob", "rank": 1}, {"name": "Charlie", "rank": 2}]}', 'STRUCT<teacher: STRING, student: ARRAY<STRUCT<name: STRING, rank: INT>>>');

## get_json_object

    get_json_object(json_txt, path)

Extracts a json object from path.

    SELECT get_json_object('{"a":"b"}', '$.a');

## json_array_length

    json_array_length(jsonArray)

Returns the number of elements in the outermost JSON array.

`Arguments:`

jsonArray - A JSON array.
NULL is returned in case of any other valid JSON string, NULL or an invalid JSON.

    SELECT json_array_length('[1,2,3,4]');
    SELECT json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]');
    SELECT json_array_length('[1,2]');

## json_object_keys

    json_object_keys(json_object)

Returns all the keys of the outermost JSON object as an array.

Arguments:

json_object - A JSON object. If a valid JSON object is given, all the keys of the outermost object will be returned as an array. If it is any other valid JSON string, an invalid JSON string or an empty string, the function returns null.

    SELECT json_object_keys('{}'); 
    SELECT json_object_keys('{"key": "value"}');
    SELECT json_object_keys('{"f1":"abc","f2":{"f3":"a", "f4":"b"}}');

## json_tuple

    json_tuple(jsonStr, p1, p2, ..., pn)

Returns a tuple like the function get_json_object, but it takes multiple names.

All the input parameters and output column types are string.

    SELECT json_tuple('{"a":1, "b":2}', 'a', 'b');

## schema_of_json

    schema_of_json(json[, options])

Returns schema in the DDL format of JSON string.

    SELECT schema_of_json('[{"col":0}]');
    SELECT schema_of_json('[{"col":01}]', map('allowNumericLeadingZeros', 'true'));
