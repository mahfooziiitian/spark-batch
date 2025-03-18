from pyspark.sql import SparkSession
from pyspark.sql.functions import array, lit, struct

# Initialize Spark Session
spark = SparkSession.builder.appName("MapContainsExample").getOrCreate()

# Sample DataFrame with Country and State
data = [
    (1, "USA", "California"),
    (2, "USA", "Texas"),
    (3, "India", "Maharashtra"),
    (4, "India", "Karnataka"),
    (5, "Canada", "Ontario"),
    (6, "Canada", "Quebec"),
]

df = spark.createDataFrame(data, ["id", "country", "state"])
df.createOrReplaceTempView("locations")

# 🔹 Create List of (Country, State) Pairs
filter_list = [("USA", "California"), ("India", "Karnataka")]

# 🔹 Convert List to Array of Maps
filter_array = array(
    [struct(lit("country"), lit(c), lit("state"), lit(s)) for c, s in filter_list]
)

# 🔹 Use `map_contains()` to Filter the DataFrame
query = f"""
    SELECT * FROM locations
    WHERE EXISTS (
        SELECT FILTER({filter_array}, x -> map_contains(x, 'country') AND map_contains(x, 'state')
                      AND x['country'] = country AND x['state'] = state)
    )
"""

print(query)

df_filtered = spark.sql(query)
df_filtered.show()
