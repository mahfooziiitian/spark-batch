# regexp

    regexp(str, regexp)

Returns true if str matches regexp, or false otherwise.

`Arguments:`

`str` - a string expression

`regexp` - a string expression. The regex string should be a Java regular expression.

    SET spark.sql.parser.escapedStringLiterals=true;
    SELECT regexp('%SystemDrive%\Users\John', '%SystemDrive%\\Users.*');

    SET spark.sql.parser.escapedStringLiterals=false;
    SELECT regexp('%SystemDrive%\\Users\\John', '%SystemDrive%\\\\Users.*');
