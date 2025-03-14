package com.mahfooz.spark.sqlserver.util;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class JdbcSchemaUtils {

    public static StructType createSparkSchemaFromResultSetMetadata(ResultSet resultSet)
            throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int numColumns = metaData.getColumnCount();
        StructField[] fields = new StructField[numColumns];

        for (int i = 1; i <= numColumns; i++) {
            String columnName = metaData.getColumnName(i);
            int columnType = metaData.getColumnType(i);

            // Map JDBC types to Spark SQL DataTypes
            DataType dataType = mapJdbcTypeToSparkType(columnType);

            // Create StructField for the column
            fields[i - 1] = DataTypes.createStructField(columnName, dataType, true);
        }

        return DataTypes.createStructType(fields);
    }

    private static DataType mapJdbcTypeToSparkType(int jdbcType) {
        // Map JDBC types to Spark SQL DataTypes
        switch (jdbcType) {
            case java.sql.Types.INTEGER:
                return DataTypes.IntegerType;
            case java.sql.Types.BIGINT:
                return DataTypes.LongType;
            case java.sql.Types.FLOAT:
                return DataTypes.FloatType;
            case java.sql.Types.DOUBLE:
                return DataTypes.DoubleType;
            case java.sql.Types.BOOLEAN:
                return DataTypes.BooleanType;
            case java.sql.Types.DATE:
                return DataTypes.DateType;
            case java.sql.Types.TIMESTAMP:
                return DataTypes.TimestampType;
            case Types.DECIMAL:
                return DataTypes.createDecimalType(18,2);
            default:
                return DataTypes.StringType;
        }
    }
}
