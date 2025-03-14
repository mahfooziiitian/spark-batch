package com.mahfooz.df.model;

import lombok.*;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Setter
@Getter
@ToString
public  class Person implements Serializable {
    private int id;
    private String name;
    private int age;

}
