package com.mahfooz.spark.jdbc.util;

import org.jasypt.encryption.pbe.PooledPBEStringEncryptor;

import java.util.Base64;

public class EncryptionUtil {

    private PooledPBEStringEncryptor encryptor;

    public PooledPBEStringEncryptor getEncryptor() {
        return encryptor;
    }

    public EncryptionUtil (String encryptionPassword,String algorithm){
        encryptor = new PooledPBEStringEncryptor();
        encryptor.setPoolSize(4);
        encryptor.setPassword(new String(Base64.getDecoder().decode(encryptionPassword)));
        encryptor.setAlgorithm(algorithm);
    }


    public String encrypt(String input) {
        String encodedInput=new String(Base64.getEncoder().encode(input.getBytes()));
        return encryptor.encrypt(encodedInput);
    }

    public  String decrypt(String encryptedMessage) {
        String encodedOutput=encryptor.decrypt(encryptedMessage);
        return new String(Base64.getDecoder().decode(encodedOutput));
    }

}