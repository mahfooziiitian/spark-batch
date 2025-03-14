package com.mahfooz.spark.model;

import java.io.Serializable;
import java.util.Objects;

public class Apple implements Serializable {

    private static final long serialVersionUID = 1L;
    private String color;
    private Integer weight;

    public Apple(String color, Integer weight) {
        this.color = color;
        this.weight = weight;
    }

    public String getColor() {
        return color;
    }

    // Setters are required in Java object for parallelize
    public void setColor(String color) {
        this.color = color;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }

    public Integer getWeight() {
        return weight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Apple apple = (Apple) o;
        return Objects.equals(color, apple.color) && Objects.equals(weight, apple.weight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(color, weight);
    }

    @Override
    public String toString() {
        return "Apple{" + "color='" + color + '\'' + ", weight=" + weight + '}';
    }
}
