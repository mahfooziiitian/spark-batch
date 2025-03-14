from pyspark.sql import SparkSession
from jproperties import Properties

if __name__ == '__main__':
    spark = (SparkSession.builder
             .appName("config_file")
             .master("local[*]")
             .getOrCreate())

    configs = Properties()

    with open('../../../cfg/config.properties', 'rb') as config_file:
        configs.load(config_file)

    try:
        random_value = configs["a"]
        print(random_value.data)
        random_value = f"{{configs['b'].data}}"
        print(random_value)
    except KeyError as ke:
        print(f'{ke}, lookup key was "Random_Key"')
