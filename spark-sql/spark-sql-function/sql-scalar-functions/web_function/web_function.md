# Web

## parse_url

    parse_url(url, partToExtract[, key])

Extracts a part from a URL.

    SELECT parse_url('http://spark.apache.org/path?query=1', 'HOST');
    SELECT parse_url('http://spark.apache.org/path?query=1', 'QUERY');
    SELECT parse_url('http://spark.apache.org/path?query=1', 'QUERY', 'query');
