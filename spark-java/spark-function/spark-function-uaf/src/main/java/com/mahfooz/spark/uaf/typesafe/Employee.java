package com.mahfooz.spark.uaf.typesafe;

import java.io.Serializable;

public  class Employee implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private String name;
    private long salary;

    public Employee(){

    }
    
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
    public Employee(String name, long salary) {
        this.name = name;
        this.salary = salary;
    }
  
  }