package com.mahfooz.saprk.prquet.model;

import java.io.Serializable;

public  class Square implements Serializable {
    private int value;
    private int square;

    // Getters and setters


    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getSquare() {
        return square;
    }

    public void setSquare(int square) {
        this.square = square;
    }
}