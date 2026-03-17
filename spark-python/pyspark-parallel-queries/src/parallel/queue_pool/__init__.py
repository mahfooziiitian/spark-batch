"""Queue-based bounded worker pool — TOCTOU-safe drain, progress tracking."""
from .queue_worker import worker, load_table

__all__ = ["worker", "load_table"]
