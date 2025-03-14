/*
This is a functional interface and can therefore be used as the assignment target for a lambda expression or method reference.
 */
package com.mahfooz.spark.function.reduce;

import org.apache.spark.api.java.function.ReduceFunction;

public class ReduceFunc implements ReduceFunction {

    @Override
    public Object call(Object v1, Object v2) throws Exception {
        return null;
    }
}
