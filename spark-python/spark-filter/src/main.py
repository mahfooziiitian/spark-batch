def main():
    or_equal = False
    epxression = eval(
        "'(temp_column_A >= temp_column_B) | (temp_column_A.eqNullSafe(temp_column_B))' if or_equal else 'temp_column_A > temp_column_B'"
    )
    print(epxression)

    expression = " "


if __name__ == "__main__":
    main()
