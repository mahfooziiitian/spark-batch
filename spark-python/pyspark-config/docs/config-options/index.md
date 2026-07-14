# Config Options

PySpark applications often need external configuration — database URLs, file paths,
queue names, or custom business parameters. This section covers three approaches
for loading configuration from files.

## Approaches

| Approach | Library | File Format | Best For |
|----------|---------|-------------|----------|
| [ConfigParser](configparser.md) | stdlib `configparser` | `.cfg` / `.conf` (INI-style) | Simple key-value configs with sections and interpolation |
| [jproperties](jproperties.md) | `jproperties` | `.properties` (Java-style) | Java-compatible properties files |
| [Spark Options](spark-options.md) | PySpark built-in | — | Inspecting all runtime Spark config keys |

## Config File Formats

### INI-style (`.cfg`)

```ini title="cfg/config.cfg"
--8<-- "cfg/config.cfg"
```

### INI-style with Extended Interpolation (`.conf`)

```ini title="cfg/config.conf"
--8<-- "cfg/config.conf"
```

### Java Properties (`.properties`)

```properties title="cfg/config.properties"
--8<-- "cfg/config.properties"
```

!!! note
    The `.cfg` file uses `%(key)s` interpolation (Python `BasicInterpolation`),
    while `.conf` uses `${section:key}` (`ExtendedInterpolation`).
    The `.properties` file is flat key-value pairs with no interpolation.
