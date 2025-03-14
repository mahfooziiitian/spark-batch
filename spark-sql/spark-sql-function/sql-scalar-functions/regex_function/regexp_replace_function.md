# regexp_replace

    regexp_replace(str, regexp, rep[, position])

Replaces all substrings of str that match regexp with rep.

    SELECT regexp_replace('100-200', '(\\d+)', 'num');
