def fahrenheit_to_centigrade(temperature):
    """
    Convert temperature from Fahrenheit to Centigrade.

    Args:
        temperature (float or int): Temperature in Fahrenheit.

    Returns:
        float: Temperature in Centigrade.

    Raises:
        ValueError: If input is not a number.
    """
    try:
        return (float(temperature) - 32) * 5 / 9
    except (TypeError, ValueError):
        raise ValueError("Input temperature must be a number.")


def fahrenheit_to_kelvin(temperature):
    """
    Convert temperature from Fahrenheit to Kelvin.

    Args:
        temperature (float or int): Temperature in Fahrenheit.

    Returns:
        float: Temperature in Kelvin.

    Raises:
        ValueError: If input is not a number.
    """
    try:
        return (float(temperature) - 32) * 5 / 9 + 273.15
    except (TypeError, ValueError):
        raise ValueError("Input temperature must be a number.")


def main():
    temps_f = [32, 68, 100, 212, -40]
    temps_c = [fahrenheit_to_centigrade(temp) for temp in temps_f]
    temps_k = [fahrenheit_to_kelvin(temp) for temp in temps_f]
    print("Fahrenheit:", temps_f)
    print("Centigrade:", temps_c)
    print("Kelvin:", temps_k)


if __name__ == "__main__":
    main()