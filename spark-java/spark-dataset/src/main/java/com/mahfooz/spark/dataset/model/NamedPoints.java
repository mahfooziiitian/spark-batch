package com.mahfooz.spark.dataset.model;

import java.io.Serializable;
import java.util.Map;

public class NamedPoints implements Serializable {
    private static final long serialVersionUID = 1L;
    private String name;
    private Map<String, Point> points;

    public NamedPoints(String name, Map<String, Point> points) {
        this.name = name;
        this.points = points;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, Point> getPoints() {
        return points;
    }

    public void setPoints(Map<String, Point> points) {
        this.points = points;
    }
}
