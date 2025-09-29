def check_vowel_function_tuple(letter):
    """
    Returns a tuple (letter, 1) if the letter is a vowel, otherwise (letter, 0).
    Case-insensitive.
    """
    vowel_set = {"a", "e", "i", "o", "u"}
    is_vowel = 1 if letter.lower() in vowel_set else 0
    return letter, is_vowel


def check_vowel_function(letter):
    """
    Returns 1 if the letter is a vowel, otherwise 0.
    Case-insensitive.
    """
    vowel_set = {"a", "e", "i", "o", "u"}
    return 1 if letter.lower() in vowel_set else 0
