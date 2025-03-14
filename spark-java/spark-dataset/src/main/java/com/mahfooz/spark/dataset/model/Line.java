package com.mahfooz.spark.dataset.model;

import java.io.Serializable;

//
// A JavaBean for Example 2
//
public class Line implements Serializable {

    private static final long serialVersionUID = 1L;
    private String name;
    private Point[] points;

    public Line(String name, Point[] points) {
        this.name = name;
        this.points = points;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Point[] getPoints() {
        return points;
    }

    public void setPoints(Point[] points) {
        this.points = points;
    }
}
