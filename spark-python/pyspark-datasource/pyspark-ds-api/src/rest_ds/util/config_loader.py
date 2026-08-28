import os

import yaml

from rest_ds.util.config_validator import validate_config

# Default base directory for all example input/output data (JSON extracts,
# mock-server SQLite databases, incremental state stores, generated
# certificates, etc.) when the `DATA_HOME` environment variable is unset —
# matches the repo-wide "/tmp/... fallback" convention.
DEFAULT_DATA_HOME = (
    "/tmp"  # nosec B108 -- documented repo-wide fallback, not a temp-file race
)


def load_config(config_path):
    """Load a REST API ingestion YAML config, expanding ``${VAR}``/``$VAR``
    environment variable references (e.g. ``${DATA_HOME}``) before parsing,
    then validate it.
    """
    os.environ.setdefault("DATA_HOME", DEFAULT_DATA_HOME)
    with open(config_path, "r", encoding="utf-8") as f:
        raw = f.read()
    config = yaml.safe_load(os.path.expandvars(raw))
    validate_config(config)
    return config
