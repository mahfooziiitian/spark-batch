"""threading.Thread pattern — wraps Spark actions in Python threads for FAIR-scheduled concurrency."""
from .word_char_count import run_parallel, run_serial, build_words_df

__all__ = ["run_parallel", "run_serial", "build_words_df"]
