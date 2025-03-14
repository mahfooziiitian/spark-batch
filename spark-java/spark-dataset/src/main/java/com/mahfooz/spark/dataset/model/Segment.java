package com.mahfooz.spark.dataset.model;

import java.io.Serializable;

//
// A JavaBean for Example 1
//
public class Segment implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private Point from;
    private Point to;

    public Segment(Point from, Point to) {
        this.to = to;
        this.from = from;
    }

    public Point getFrom() {
        return from;
    }

    public void setFrom(Point from) {
        this.from = from;
    }

    public Point getTo() {
        return to;
    }

    public void setTo(Point to) {
        this.to = to;
    }
}
