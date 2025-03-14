package com.mahfooz.spark.sqlserver.reader.procedure;

import com.mahfooz.spark.sqlserver.connection.ConnectionManager;
import com.mahfooz.spark.sqlserver.parameter.Parameter;
import com.mahfooz.spark.sqlserver.util.JasyptEncryptDecrypt;
import com.mahfooz.spark.sqlserver.util.JdbcSchemaUtils;
import com.mahfooz.spark.sqlserver.util.JdbcUtils;
import com.mahfooz.spark.sqlserver.util.ProcedureUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CallingProcedure {

    public static void main(String[] args) throws SQLException {

        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("CallingProcedure")
                .getOrCreate();

        Config config = ConfigFactory.load();
        String secretKey = System.getenv("JASYPT_SECRET_KEY");
        String password = JasyptEncryptDecrypt.decryptPassword(
                config.getString("db.user.password"),
                secretKey
        );

        //Parameters to procedure
        List<Parameter> parameters =new ArrayList<>();
        parameters.add(new Parameter("lastNameStartsWith","B",""));
        String procedureName = "[gpt].[uspGetEmployeesByLastName]";
        String storedProcedureSql = ProcedureUtil.getProcedureCallWithParam(
                procedureName,
                parameters
        );

        try(Connection connection = ConnectionManager.getConnection()){
            try(CallableStatement callableStatement=connection.prepareCall(storedProcedureSql)){
                callableStatement.execute();
                ResultSet resultSet=callableStatement.getResultSet();
                StructType schema = JdbcSchemaUtils.createSparkSchemaFromResultSetMetadata(resultSet);
                Dataset<Row> df = JdbcUtils.createDataframeFromResultSet(sparkSession, resultSet, schema);
                df.show(100);
                df.printSchema();
            }
        }
        sparkSession.stop();
    }
}
