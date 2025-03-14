# regexp_instr

    regexp_instr(str, regexp)

Searches a string for a regular expression and returns an integer that indicates the beginning position of the matched substring. Positions are 1-based, not 0-based. If no match is found, returns 0.

     SELECT regexp_instr('user@spark.apache.org', '@[^.]*');
