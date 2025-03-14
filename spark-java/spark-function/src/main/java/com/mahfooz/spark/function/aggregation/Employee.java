package com.mahfooz.spark.function.aggregation;

import java.io.Serializable;

public  class Employee implements Serializable {

    private String name;
    private long salary;

    // Constructors, getters, setters...
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getSalary() {
        return salary;
    }

    public void setSalary(long salary) {
        this.salary = salary;
    }
}
