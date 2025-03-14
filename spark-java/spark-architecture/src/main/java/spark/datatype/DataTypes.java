/*

val playersDF = spark.createDF(List(("India", Array("Sachin", "Dhoni")),
	("Australia", Array("Ponting"))), List(("team_name", StringType, true),
	("top_players", ArrayType(StringType, true), true))
)


val spark = SparkSession.builder().appName("Java Spark SQL data").master("local[*]").getOrCreate

val people = spark.read().json("People.json");

Create DataSet with Json data


*/