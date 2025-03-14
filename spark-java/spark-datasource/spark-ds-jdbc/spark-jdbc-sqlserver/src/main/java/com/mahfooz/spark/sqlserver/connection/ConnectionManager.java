package com.mahfooz.spark.sqlserver.connection;

import com.mahfooz.spark.sqlserver.util.JasyptEncryptDecrypt;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionManager {

    public static Connection getConnection(String url, Properties properties) throws SQLException {
        return DriverManager.getConnection(url, properties);
    }

    public static Connection getConnection() {
        Config config = ConfigFactory.load();
        Connection con = null;
        try {
            String secretKey = System.getenv("JASYPT_SECRET_KEY");
            String decryptedPassword = JasyptEncryptDecrypt.decryptPassword(
                    config.getString("db.user.password"),
                    secretKey
            );
            con = DriverManager.getConnection(
                    config.getString("db.url"),
                    config.getString("db.user.id"),
                    decryptedPassword);
        } catch (SQLException ex) {
            Logger.getLogger(ConnectionManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return con;
    }
}
