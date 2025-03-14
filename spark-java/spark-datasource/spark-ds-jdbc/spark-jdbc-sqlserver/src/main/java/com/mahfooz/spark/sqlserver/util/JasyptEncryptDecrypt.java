package com.mahfooz.spark.sqlserver.util;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

public class JasyptEncryptDecrypt {

    public static String encryptPassword(String password, String secretKey) {
        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
        encryptor.setPassword(secretKey);
        return encryptor.encrypt(password);
    }

    public static String decryptPassword(String encryptedPassword, String secretKey) {
        StandardPBEStringEncryptor decryptor = new StandardPBEStringEncryptor();
        decryptor.setPassword(secretKey);
        return decryptor.decrypt(encryptedPassword.replace("ENC(", "")
                .replace(")", ""));
    }

}
