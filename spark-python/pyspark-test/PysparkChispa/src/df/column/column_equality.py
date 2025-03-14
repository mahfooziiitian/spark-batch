from pyspark.sql import functions as f


def remove_non_word_characters(col):
    return f.regexp_replace(col, "[^\\w\\s]+", "")


