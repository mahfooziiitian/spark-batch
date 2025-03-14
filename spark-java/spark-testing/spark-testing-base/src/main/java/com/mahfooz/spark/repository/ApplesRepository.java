package com.mahfooz.spark.repository;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.sum;

public class ApplesRepository {

    public long mass(Dataset<Row> fruits) {
        return fruits.select(sum("weight"))
                .as(Encoders.LONG()).first();
    }

}
