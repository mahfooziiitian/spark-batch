# regexp_extract

    regexp_extract(str, regexp[, idx])

Extract the first string in the str that match the regexp expression and corresponding to the regex group index.

`Arguments:`

str - a string expression.

regexp - a string representing a regular expression. The regex string should be a Java regular expression.

    SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 1);

## regexp_extract_all

    regexp_extract_all(str, regexp[, idx])

Extract all strings in the str that match the regexp expression and corresponding to the regex group index.

    SELECT regexp_extract_all('100-200, 300-400', '(\\d+)-(\\d+)', 1);
