package com.mahfooz.spark.function.user.udf.one;

import org.apache.spark.sql.api.java.UDF1;

public class StringLengthUDF   implements UDF1<String, Integer> {

    @Override
    public Integer call(String s) throws Exception {
        return s.length();
    }
}
