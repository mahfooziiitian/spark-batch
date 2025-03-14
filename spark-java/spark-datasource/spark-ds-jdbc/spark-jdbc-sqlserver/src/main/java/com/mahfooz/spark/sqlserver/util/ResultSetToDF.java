package com.mahfooz.spark.sqlserver.util;

import com.mahfooz.spark.sqlserver.connection.ConnectionManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

public class ResultSetToDF {

    public static Row convertResultSetToRow(ResultSet resultSet)  {
        try {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            List<String> columnNames = new ArrayList<>();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++)
                columnNames.add(resultSetMetaData.getColumnName(i));

            List<String> resultSetRecord = new ArrayList<>();
            for (String column : columnNames)
                resultSetRecord.add(resultSet.getString(column));
            return RowFactory.create(resultSetRecord);
        }
        catch (SQLException e){
            System.out.println(e.getMessage());
        }
        return null;
    }

    public static Iterator<Row> resultSetToIterator(ResultSet resultSet, Function<ResultSet, Row> convertResultSetToRow) {
        return new Iterator<Row>() {
            @Override
            public boolean hasNext() {
                try {
                    return resultSet.next();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Row next() {
                return convertResultSetToRow.apply(resultSet);
            }
        };
    }

    public static Dataset<Row> getDatasetFromProcedure(SparkSession sparkSession,
                                                       ResultSet resultSet,
                                                       StructType schema) throws SQLException {
        return JdbcUtils.createDataframeFromResultSet(sparkSession,
                resultSet, schema);
    }

    public static void main(String[] args) throws SQLException {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("ResultSetToDF")
                .master("local[*]")
                .getOrCreate();

        try(Connection connection = ConnectionManager.getConnection()){

        }
    }
}
