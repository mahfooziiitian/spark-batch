
package com.mahfooz.spark.dataset.model;

// This must be a JavaBean in order for Spark to infer a schema for it

import java.io.Serializable;

public  class Number implements Serializable {

    private static final long serialVersionUID = 1L;
    private int i;
    private String english;
    private String french;

    public Number(int i, String english, String french) {
        this.i = i;
        this.english = english;
        this.french = french;
    }

    public int getI() {
        return i;
    }

    public void setI(int i) {
        this.i = i;
    }

    public String getEnglish() {
        return english;
    }

    public void setEnglish(String english) {
        this.english = english;
    }

    public String getFrench() {
        return french;
    }

    public void setFrench(String french) {
        this.french = french;
    }
}
