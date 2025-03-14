package com.mahfooz.spark.dataframe.model;

import java.io.Serializable;

public  class Cube implements Serializable {

    private int value;
    private int cube;

    // Getters and setters...
    // $example off:schema_merging$
    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getCube() {
        return cube;
    }

    public void setCube(int cube) {
        this.cube = cube;
    }
    // $example on:schema_merging$
}
// $example off:schema_merging$