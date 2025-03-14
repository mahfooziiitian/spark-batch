# xml function

## xpath

    xpath(xml, xpath)

Returns a string array of values within the nodes of xml that match the XPath expression.

    SELECT xpath('<a><b>b1</b><b>b2</b><b>b3</b><c>c1</c><c>c2</c></a>','a/b/text()');

## xpath_boolean

    xpath_boolean(xml, xpath)

Returns true if the XPath expression evaluates to true, or if a matching node is found.

    SELECT xpath_boolean('<a><b>1</b></a>','a/b');

## xpath_double

    xpath_double(xml, xpath)

Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.

    SELECT xpath_double('<a><b>1</b><b>2</b></a>', 'sum(a/b)');

## xpath_float

    xpath_float(xml, xpath)

Returns a float value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.

    SELECT xpath_float('<a><b>1</b><b>2</b></a>', 'sum(a/b)');

## xpath_int

    xpath_int(xml, xpath)

Returns an integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.

    SELECT xpath_int('<a><b>1</b><b>2</b></a>', 'sum(a/b)');

## xpath_long

    xpath_long(xml, xpath)

Returns a long integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.

    SELECT xpath_long('<a><b>1</b><b>2</b></a>', 'sum(a/b)');

## xpath_number

    xpath_number(xml, xpath)

Returns a double value, the value zero if no match is found, or NaN if a match is found but the value is non-numeric.

    SELECT xpath_number('<a><b>1</b><b>2</b></a>', 'sum(a/b)');

## xpath_short

    xpath_short(xml, xpath)

Returns a short integer value, or the value zero if no match is found, or a match is found but the value is non-numeric.

    SELECT xpath_short('<a><b>1</b><b>2</b></a>', 'sum(a/b)');

## xpath_string

    xpath_string(xml, xpath)

Returns the text contents of the first xml node that matches the XPath expression.

    SELECT xpath_string('<a><b>b</b><c>cc</c></a>','a/c');
