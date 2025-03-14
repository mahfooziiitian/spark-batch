package com.mahfooz.spark.function.filter;

import org.apache.spark.api.java.function.FilterFunction;

public class FilterFunc implements FilterFunction {

    @Override
    public boolean call(Object value) throws Exception {
        return false;
    }
}
