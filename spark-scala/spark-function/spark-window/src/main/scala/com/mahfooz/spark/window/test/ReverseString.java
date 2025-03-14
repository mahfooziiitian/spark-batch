package com.mahfooz.spark.window.test;

import java.util.Arrays;
import java.util.stream.Collectors;

public class ReverseString {

    public static String reverse(String str){
        StringBuilder builder=new StringBuilder();
        for(int i=str.length()-1;i>=0;i--){
            builder.append(str.charAt(i));
        }
        return builder.toString();
    }

    public static void main(String[] args) {
        String str="I am 11 years of experience";
        System.out.println(Arrays.stream(reverse(str).split(" ")).map(record->reverse(record)).collect(Collectors.joining(" ")));
    }
}
