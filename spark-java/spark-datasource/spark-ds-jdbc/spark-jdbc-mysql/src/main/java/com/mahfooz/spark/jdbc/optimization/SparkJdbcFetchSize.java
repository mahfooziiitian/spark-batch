package com.mahfooz.spark.jdbc.optimization;

import com.mahfooz.spark.jdbc.config.SparkJdbcConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Properties;

public class SparkJdbcFetchSize {

    public static void main(String[] args) throws IOException {

        Properties properties= SparkJdbcConfig.getProperties("D:\\data\\database\\mysql\\mysql-db-java.properties",
                "bW92ZV9leHRlcm5hbF9hcGk=",
                "PBEWithMD5AndTripleDES");

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("SparkJdbcFetchSize")
                .getOrCreate();

        Dataset<Row> jdbcDF=spark.read()
                .format("jdbc")
                .option("url",properties.getProperty("url"))
                .option("dbtable", "ontime.ontime_part")
                .option("fetchSize", "10000")
                .option("partitionColumn" , "yeard")
                .option("lowerBound" , "1988")
                .option("upperBound" ,"2016")
                .option("numPartitions","28")
                .load();

        jdbcDF.createOrReplaceTempView("ontime");

        Dataset<Row> sqlDF = spark.sql("select min(year), max(year) as max_year, " +
                "Carrier, count(*) as cnt, sum(if(ArrDelayMinutes>30, 1, 0)) as flights_delayed, " +
                "round(sum(if(ArrDelayMinutes>30, 1, 0))/count(*),2) as rate FROM ontime WHERE DayOfWeek not in (6,7) " +
                "and OriginState not in ('AK', 'HI', 'PR', 'VI') and DestState not in ('AK', 'HI', 'PR', 'VI') " +
                "and (origin = 'RDU' or dest = 'RDU') GROUP by carrier HAVING cnt > 100000 and max_year > '1990' " +
                "ORDER by rate DESC, cnt desc LIMIT  10");
        sqlDF.show();

    }
}
