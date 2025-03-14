package com.mahfooz.spark.sqlserver.util;

import com.mahfooz.spark.sqlserver.connection.ConnectionManager;
import com.mahfooz.spark.sqlserver.parameter.Parameter;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ProcedureUtil {

    public static String getProcedureCallWithParam(String name, List<Parameter> parameterList){
        String procedureStatement = parameterList
                .stream()
                .map(parameter->"'%s'")
                .collect(Collectors.joining(","));

        procedureStatement = "{ CALL "+name + "( "+procedureStatement+" )}";

        System.out.println(procedureStatement);

        Object[] paramValues = parameterList.stream()
                .map(Parameter::getValue).toArray();

        System.out.println(Arrays.toString(paramValues));
        return String.format(procedureStatement,paramValues);
    }

    public static void main(String[] args) {
        List<Parameter> parameterList =new ArrayList<>();
        parameterList.add(new Parameter("admin","admin1",""));
        String storedProcedureSql = getProcedureCallWithParam("procName",parameterList);
        System.out.println(getProcedureCallWithParam("procName",parameterList));
        try(Connection con = ConnectionManager.getConnection()){
            try(CallableStatement callableStatement = con.prepareCall(storedProcedureSql)){
                callableStatement.execute();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
