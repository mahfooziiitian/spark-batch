def temp_more_than_thirteen(temperature):
    """
    Checks if the given temperature is greater than or equal to 13.

    Args:
        temperature (float or int): The temperature value to check.

    Returns:
        bool: True if temperature >= 13, False otherwise.
    """
    try:
        return float(temperature) >= 13
    except (TypeError, ValueError):
        return False
