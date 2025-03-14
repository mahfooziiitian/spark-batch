# Encryption function

## crc32

    crc32(expr)

It returns a cyclic redundancy check value of the expr as a bigint.

    SELECT crc32('Spark');

## encode

    encode(str, charset)

Encodes the first argument using the second argument character set.

    SELECT encode('abc', 'utf-8');
