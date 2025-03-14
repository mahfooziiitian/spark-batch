def checkVowelFunctionTuple(letter):
    vowel_list = ['a', 'e', 'i', 'o', 'u']
    if letter in vowel_list:
        return letter, 1
    else:
        return letter, 0


def checkVowelFunction(letter):
    vowel_list = ['a', 'e', 'i', 'o', 'u']
    if letter in vowel_list:
        return 1
    else:
        return 0
