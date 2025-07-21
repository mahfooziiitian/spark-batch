# regexp_like

    regexp_like(str, regexp)

Returns true if str matches regexp, or false otherwise.

    SET spark.sql.parser.escapedStringLiterals=true;
    SELECT regexp_like('%SystemDrive%\Users\John', '%SystemDrive%\\Users.*');
    SET spark.sql.parser.escapedStringLiterals=false;
    SELECT regexp_like('%SystemDrive%\\Users\\John', '%SystemDrive%\\\\Users.*');
