package com.mahfooz.spark.sqlserver.connection;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class ConnectionManagerTest {

    @Test
    public void testConnection() throws SQLException {
        try(Connection connection=ConnectionManager.getConnection()){
            assertNotNull(connection);
            assertFalse(connection.isClosed());
        }
    }
}