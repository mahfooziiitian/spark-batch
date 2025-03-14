package com.mahfooz.spark.jdbc.util;

import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class Base64Test {

    @Test
    public void givenValue_WhenEncodeThenDecode_ThenItShouldBeEqual(){
        //Given
        String original="move_external_api";

        //When
        String encodedValue= Base64.getEncoder().encodeToString(original.getBytes());
        System.out.println(encodedValue);
        String encodedThenDecoded= new String(Base64.getDecoder().decode(encodedValue));

        //Then
        assertEquals(original,encodedThenDecoded);
    }
}
