if __name__ == '__main__':
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    even_numbers = list(filter(lambda n: n % 2 == 0, numbers))
    print(even_numbers)
    field ="name"
    exploded_array = [f"explode_outer( {field} ) as {explode_outer}"]
