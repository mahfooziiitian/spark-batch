package com.mahfooz.df.model;

import lombok.*;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class Person implements Serializable {
    private String name;
    private int age;
}
