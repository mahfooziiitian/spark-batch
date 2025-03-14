package com.mahfooz.spark.function.user.udf.zero;

import org.apache.spark.sql.api.java.UDF0;

public class UdfZero implements UDF0 {

    @Override
    public Object call() throws Exception {
        return null;
    }
}
