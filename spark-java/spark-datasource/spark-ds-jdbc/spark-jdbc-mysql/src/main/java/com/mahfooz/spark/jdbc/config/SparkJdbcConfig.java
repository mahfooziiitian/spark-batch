package com.mahfooz.spark.jdbc.config;

import com.mahfooz.spark.jdbc.util.EncryptionUtil;
import org.jasypt.encryption.pbe.PooledPBEStringEncryptor;
import org.jasypt.properties.EncryptableProperties;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Properties;

public class SparkJdbcConfig {

    public static Properties getProperties(String filename,String encryptionPassword,String algorithm)
            throws IOException {
        PooledPBEStringEncryptor encryptor=new EncryptionUtil (encryptionPassword,algorithm).getEncryptor();
        Properties props = new EncryptableProperties(encryptor);
        props.load(Files.newInputStream(Paths.get(filename)));
        props.setProperty("password", new String(Base64.getDecoder().decode(props.getProperty("password"))));
        return props;
    }
}
