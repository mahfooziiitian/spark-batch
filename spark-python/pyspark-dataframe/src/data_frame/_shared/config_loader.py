"""
Centralised configuration loader for pyspark-dataframe.

Usage
-----
Load config and build a SparkSession from it::

    from data_frame._shared.config_loader import get_spark_from_config, setup_logging

    setup_logging()
    spark = get_spark_from_config("my-job")   # uses CONFIG_PROFILE env var (default: dev)

Load raw config values::

    from data_frame._shared.config_loader import load_config, get_paths

    cfg  = load_config()              # full dict
    paths = get_paths()               # resolved path dict
    salt  = cfg["app"]["salt_buckets"]

Profile selection
-----------------
Set the ``CONFIG_PROFILE`` environment variable to choose the YAML file:

    CONFIG_PROFILE=prod python src/data_frame/etl/etl.py

Environment variable overrides
--------------------------------
The following env vars always win over the YAML file:

    SPARK_MASTER                → spark.master
    SPARK_SHUFFLE_PARTITIONS    → spark.sql.shuffle.partitions
    INPUT_PATH / OUTPUT_PATH    → paths.input / paths.output
    CHECKPOINT_PATH             → paths.checkpoint
    LOG_PATH                    → paths.logs
"""

from __future__ import annotations

import logging
import logging.config
import os
from pathlib import Path
from typing import Any, Optional

import yaml
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Path constants
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parents[3]  # …/pyspark-dataframe/
_CONFIGS_DIR = _PROJECT_ROOT / "configs"

_DEFAULT_PROFILE = "dev"

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _expand_env(value: Any) -> Any:
    """Recursively expand ``${VAR}`` placeholders inside string values."""
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def _apply_env_overrides(cfg: dict) -> dict:
    """Apply well-known environment variable overrides on top of YAML values."""
    spark_cfg = cfg.setdefault("spark", {})
    spark_cfg_configs = spark_cfg.setdefault("configs", {})

    if master := os.environ.get("SPARK_MASTER"):
        spark_cfg["master"] = master

    if partitions := os.environ.get("SPARK_SHUFFLE_PARTITIONS"):
        spark_cfg_configs["spark.sql.shuffle.partitions"] = partitions

    paths = cfg.setdefault("paths", {})
    for env_var, path_key in [
        ("INPUT_PATH", "input"),
        ("OUTPUT_PATH", "output"),
        ("CHECKPOINT_PATH", "checkpoint"),
        ("LOG_PATH", "logs"),
    ]:
        if val := os.environ.get(env_var):
            paths[path_key] = val

    return cfg


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_config(profile: Optional[str] = None) -> dict:
    """Load and return the merged configuration dict for *profile*.

    Resolution order (highest priority first):
    1. Environment variable overrides
    2. YAML file values
    3. Built-in defaults

    Args:
        profile: Config profile name — matches ``configs/<profile>.yaml``.
                 Falls back to the ``CONFIG_PROFILE`` env var, then ``"dev"``.

    Returns:
        Fully resolved configuration dict.

    Raises:
        FileNotFoundError: When the YAML file for the requested profile does
            not exist.
    """
    profile = profile or os.environ.get("CONFIG_PROFILE", _DEFAULT_PROFILE)
    config_path = _CONFIGS_DIR / f"{profile}.yaml"

    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            f"Available profiles: {[p.stem for p in _CONFIGS_DIR.glob('*.yaml') if p.stem != 'logging']}"
        )

    with config_path.open() as fh:
        cfg = yaml.safe_load(fh) or {}

    cfg = _expand_env(cfg)
    cfg = _apply_env_overrides(cfg)
    return cfg


def get_paths(profile: Optional[str] = None) -> dict[str, str]:
    """Return the resolved ``paths`` section from config.

    Args:
        profile: Config profile name. Defaults to ``CONFIG_PROFILE`` env var.

    Returns:
        Dict with keys ``input``, ``output``, ``checkpoint``, ``logs``.
    """
    return load_config(profile).get("paths", {})


def get_spark_from_config(
    app_name: str,
    profile: Optional[str] = None,
    extra_configs: Optional[dict] = None,
) -> SparkSession:
    """Build and return a :class:`SparkSession` driven entirely by the YAML config.

    All Spark configs in the ``spark.configs`` section of the YAML are applied.
    ``extra_configs`` are merged last and take the highest priority.

    Args:
        app_name: Value for ``spark.app.name``.
        profile:  Config profile name. Defaults to ``CONFIG_PROFILE`` env var.
        extra_configs: Optional dict of additional key/value Spark configs that
            override any YAML values.

    Returns:
        A ready-to-use :class:`SparkSession`.
    """
    cfg = load_config(profile)
    spark_section = cfg.get("spark", {})

    master = spark_section.get("master", "local[*]")
    log_level = spark_section.get("log_level", "WARN")
    spark_configs: dict = spark_section.get("configs", {})

    # extra_configs win over everything
    if extra_configs:
        spark_configs.update(extra_configs)

    builder = SparkSession.builder.appName(app_name).master(master)
    for key, value in spark_configs.items():
        builder = builder.config(key, str(value))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark


def setup_logging(log_dir: Optional[str] = None) -> None:
    """Configure Python logging from ``configs/logging.yaml``.

    Args:
        log_dir: Override the log file directory in the config. When given,
            the ``filename`` of the ``file`` and ``error_file`` handlers are
            rewritten to use this directory. Defaults to the ``paths.logs``
            value from the active config profile.

    The function is idempotent — calling it multiple times is safe.
    """
    logging_config_path = _CONFIGS_DIR / "logging.yaml"
    if not logging_config_path.exists():
        logging.basicConfig(level=logging.INFO)
        return

    with logging_config_path.open() as fh:
        log_cfg = yaml.safe_load(fh)

    # Resolve log directory
    resolved_log_dir = (
        log_dir
        or os.environ.get("LOG_PATH")
        or get_paths().get("logs", "/tmp/pyspark_df/logs")
    )
    Path(resolved_log_dir).mkdir(parents=True, exist_ok=True)

    # Rewrite handler filenames to the resolved directory
    for handler_name, handler_cfg in log_cfg.get("handlers", {}).items():
        if "filename" in handler_cfg:
            filename = Path(handler_cfg["filename"]).name
            handler_cfg["filename"] = str(Path(resolved_log_dir) / filename)

    logging.config.dictConfig(log_cfg)
