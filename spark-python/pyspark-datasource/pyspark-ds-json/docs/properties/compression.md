# Compression

Spark supports multiple compression codecs when writing JSON files.

## Supported Codecs

| Codec | File Extension | Use Case |
|-------|---------------|----------|
| `none` | `.json` | Development, debugging |
| `gzip` | `.json.gz` | General-purpose, good compression ratio |
| `bzip2` | `.json.bz2` | Maximum compression, slower |
| `deflate` | `.json.deflate` | Similar to gzip without headers |
| `lz4` | `.json.lz4` | Fast compression/decompression |
| `snappy` | `.json.snappy` | Balanced speed and ratio (Hadoop ecosystem) |

## Examples

```python title="examples/04_properties/01_compression.py"
--8<-- "examples/04_properties/01_compression.py"
```

## Run

```bash
python examples/04_properties/01_compression.py
```
