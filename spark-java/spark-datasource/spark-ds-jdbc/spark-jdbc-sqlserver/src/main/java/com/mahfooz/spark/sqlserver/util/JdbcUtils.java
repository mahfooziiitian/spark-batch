package com.mahfooz.spark.sqlserver.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtils {

    public static Dataset<Row> createDataframeFromResultSet(
            SparkSession sparkSession,
            ResultSet resultSet,
            StructType schema) throws SQLException {
        return  sparkSession.createDataFrame(
                JdbcUtils.resultSetToRowList(resultSet),
                schema);
    }
    public static List<Row> resultSetToRowList(ResultSet resultSet) throws SQLException {
        List<Row> rows = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int numColumns = metaData.getColumnCount();

        while (resultSet.next()) {
            Object[] values = new Object[numColumns];
            for (int i = 1; i <= numColumns; i++) {
                values[i - 1] = resultSet.getObject(i);
            }
            rows.add(RowFactory.create(values));
        }

        return rows;
    }
}



