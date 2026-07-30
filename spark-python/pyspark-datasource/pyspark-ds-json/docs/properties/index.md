# JSON Properties

Every JSON read/write option available in PySpark's JSON datasource.

## Read Options Reference

| Option | Default | Description |
|--------|---------|-------------|
| `multiline` | `false` | Parse multiline JSON (one record spans multiple lines) |
| `primitivesAsString` | `false` | Infer all primitive values as strings |
| `prefersDecimal` | `false` | Infer floating-point values as `DecimalType` |
| `allowComments` | `false` | Allow Java/C++ style comments in JSON |
| `allowUnquotedFieldNames` | `false` | Allow unquoted field names |
| `allowSingleQuotes` | `true` | Allow single quotes for strings |
| `allowNumericLeadingZeros` | `false` | Allow leading zeros in numbers |
| `allowNonNumericNumbers` | `false` | Allow `NaN`, `Infinity`, `-Infinity` |
| `allowBackslashEscapingAnyCharacter` | `false` | Allow backslash to escape any character |
| `mode` | `PERMISSIVE` | Parse mode: PERMISSIVE, DROPMALFORMED, FAILFAST |
| `columnNameOfCorruptRecord` | `_corrupt_record` | Column for corrupt records in PERMISSIVE mode |
| `dateFormat` | `yyyy-MM-dd` | Date format pattern |
| `timestampFormat` | `yyyy-MM-dd'T'HH:mm:ss` | Timestamp format pattern |
| `encoding` | `UTF-8` | Character encoding |
| `lineSep` | `\n` | Line separator |
| `samplingRatio` | `1.0` | Fraction of data to sample for schema inference |
| `dropFieldIfAllNull` | `false` | Drop columns that are all null during schema inference |
| `locale` | `en-US` | Locale for number/date parsing |

## Write Options Reference

| Option | Default | Description |
|--------|---------|-------------|
| `compression` | `none` | Compression codec: gzip, bzip2, deflate, lz4, snappy |
| `dateFormat` | `yyyy-MM-dd` | Date format for output |
| `timestampFormat` | `yyyy-MM-dd'T'HH:mm:ss` | Timestamp format for output |
| `encoding` | `UTF-8` | Output encoding |
| `lineSep` | `\n` | Line separator in output |
| `ignoreNullFields` | `true` | Omit null fields from output |

## Topics

- [Encoding](encoding.md) — UTF-8, UTF-16, UTF-32
- [Compression](compression.md) — gzip, bzip2, deflate, lz4, snappy
- [Formatting](formatting.md) — Date/time formats, timezone, timestamp NTZ
- [Null Fields](null-fields.md) — dropFieldIfAllNull, ignoreNullFields
- [Other Options](other-options.md) — Comments, quoting, locale, sampling, etc.
