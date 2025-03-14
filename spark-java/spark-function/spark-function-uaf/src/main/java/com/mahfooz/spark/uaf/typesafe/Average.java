package com.mahfooz.spark.uaf.typesafe;

import java.io.Serializable;

public class Average implements Serializable  {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private long sum;
    private long count;

    public Average() {
    }
    
    public long getSum() {
        return sum;
    }
    public void setSum(long sum) {
        this.sum = sum;
    }
    public long getCount() {
        return count;
    }
    public void setCount(long count) {
        this.count = count;
    }
    public Average(long sum, long count) {
        this.sum = sum;
        this.count = count;
    }

  }
