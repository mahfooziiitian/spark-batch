package com.mahfooz.spark.sqlserver.reader.procedure;

import com.mahfooz.spark.sqlserver.connection.ConnectionManager;
import com.mahfooz.spark.sqlserver.parameter.Parameter;
import com.mahfooz.spark.sqlserver.util.JdbcSchemaUtils;
import com.mahfooz.spark.sqlserver.util.JdbcUtils;
import com.mahfooz.spark.sqlserver.util.ProcedureUtil;
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

public class CallingProcedureTwoParameters {

    public static void main(String[] args) throws SQLException {

        SparkSession sparkSession = SparkSession.builder()
                .master("local[*]")
                .appName("CallingProcedureTwoParameters")
                .getOrCreate();

        //Parameters to procedure
        List<Parameter> parameters =new ArrayList<>();
        parameters.add(new Parameter("@teacherName","Gerardo Grimes",""));
        parameters.add(new Parameter("@grade","75",""));
        String procedureName = "[gpt].[uspGetStudentCourseGradeByTeacherAndGrade]";
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
