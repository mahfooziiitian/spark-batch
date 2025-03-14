/*

A function that returns zero or more output records from each grouping key and its values from 2 Datasets.

 */
package com.mahfooz.spark.function.group.cogroup;

import org.apache.spark.api.java.function.CoGroupFunction;

import java.util.Iterator;

public class CoGroupFunct implements CoGroupFunction<Object,Iterator,Iterator,Iterator> {

    @Override
    public Iterator call(Object key, Iterator left, Iterator right) throws Exception {
        return null;
    }
}
