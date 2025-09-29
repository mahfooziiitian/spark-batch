import re


def dots_to_underscores(s):
    """
    Replace all dots in the input string with underscores.
    Handles None input gracefully and strips leading/trailing whitespace.

    Args:
        s (str): Input string.

    Returns:
        str: String with dots replaced by underscores.
    """
    if not isinstance(s, str):
        return s
    s = s.strip()
    return re.sub(r"\.+", "_", s)
