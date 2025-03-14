package com.mahfooz.spark.jdbc.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class EncryptionUtilTest {
    private static EncryptionUtil encryptionUtil;

    @BeforeEach
    public void setup(){
        encryptionUtil=new EncryptionUtil("bW92ZV9leHRlcm5hbF9hcGk=","PBEWithMD5AndTripleDES");
    }

    @Test
    public void givenPassword_whenEncodeAndDecode_ThenItShouldBeEqual(){
        //Given
        String original="Root@2021";

        //When
        System.out.println(encryptionUtil.encrypt(original));
        String encodedThenDecoded=encryptionUtil.decrypt(encryptionUtil.encrypt(original));

        //Then
        assertEquals(original,encodedThenDecoded);
    }
}