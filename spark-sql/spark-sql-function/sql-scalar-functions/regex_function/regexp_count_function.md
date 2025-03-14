# regexp_count

    regexp_count(str, regexp)

Returns a count of the number of times that the regular expression pattern regexp is matched in the string str.

`Arguments:`

str - a string expression.

regexp - a string representing a regular expression.
The regex string should be a Java regular expression.

    SELECT regexp_count('Steven Jones and Stephen Smith are the best players', 'Ste(v|ph)en');
    SELECT regexp_count('abcdefghijklmnopqrstuvwxyz', '[a-z]{3}');
