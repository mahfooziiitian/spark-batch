import re


def dots_to_underscores(s: str) -> str:
    """Replace all dots with underscores in a string.

    Args:
        s: Input string.

    Returns:
        String with dots replaced by underscores.
    """
    return s.replace(".", "_")


def snake_case(s: str) -> str:
    """Convert a string to snake_case.

    Replaces spaces, hyphens, and dots with underscores and lowercases the result.

    Args:
        s: Input string.

    Returns:
        Snake-cased string.
    """
    return re.sub(r"[\s\-\.]+", "_", s).lower()


def truncate(s: str, max_length: int, suffix: str = "...") -> str:
    """Truncate a string to a maximum length, appending a suffix when trimmed.

    Args:
        s: Input string.
        max_length: Maximum allowed length of the output.
        suffix: String to append when truncation occurs.

    Returns:
        Original string if within limit, otherwise truncated with suffix.

    Raises:
        ValueError: If ``max_length`` is less than the length of ``suffix``.
    """
    if max_length < len(suffix):
        raise ValueError(f"max_length ({max_length}) must be >= len(suffix) ({len(suffix)})")
    if len(s) <= max_length:
        return s
    return s[: max_length - len(suffix)] + suffix
