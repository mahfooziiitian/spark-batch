# ConfigParser

Read INI-style `.cfg` and `.conf` files using Python's built-in
[`configparser`](https://docs.python.org/3/library/configparser.html) module —
no extra dependencies needed.

## How It Works

`ConfigParser` reads sections and keys from INI files. Two interpolation modes
are available:

| Mode | Syntax | Example |
|------|--------|---------|
| Basic (default) | `%(key)s` | `select * from %(a)s` |
| Extended | `${section:key}` | `select * from ${data:a}` |

```python
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(interpolation=ExtendedInterpolation())
config.read("cfg/config.conf")

query = config.get("data", "b")  # resolves ${data:a} → "customer"
```

## Run

```bash
uv run python src/cfg/option/config_parser/config_file_configparser.py
```

Expected output:

```
=== config.cfg (BasicInterpolation) ===
a = customer
b = select * from  customer
c = select * from  ${data:a}
    select

=== config.conf (ExtendedInterpolation) ===
a = customer
b = select * from  customer
c = select * from  customer
    select
```

## Full Example

```python title="src/cfg/option/config_parser/config_file_configparser.py"
--8<-- "src/cfg/option/config_parser/config_file_configparser.py"
```

!!! note
    The script uses `SCRIPT_DIR`-relative paths so it works regardless of your
    current working directory.

!!! success "Good fit"
    - Standard library — no extra packages
    - Multi-line values supported
    - Variable interpolation between keys

!!! failure "Not a good fit"
    - Not compatible with Java `.properties` files
    - No type coercion — all values are strings
