package com.indocms.importtask.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class PostgresqlService {
    
    @Value("${app.postgresql.class}")
    private String postgresqlClass;

    @Value("${app.postgresql.uri}")
    private String postgresqlUri;

    @Value("${app.postgresql.username}")
    private String postgresqlUsername;

    @Value("${app.postgresql.password}")
    private String postgresqlPassword;

    public List<Document> executeQuery(String query) throws Exception {
        List<Document> output = new ArrayList<>();
        Connection connection = null;
        try {
            Class.forName(postgresqlClass);
            connection = DriverManager.getConnection(postgresqlUri, postgresqlUsername, postgresqlPassword);

            Statement statement = connection.createStatement();
            // String query = "select * from \"INDO_CMS_SAMPLE_REPORT\"";
            // String query = templateHeader.get("query_select").toString();
            ResultSet resultSet = statement.executeQuery(query);
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                        
            while (resultSet.next()) {
                Document row = new Document();
                for (int i = 1;i <= resultSetMetaData.getColumnCount();i++) {
                    String columnName = resultSetMetaData.getColumnLabel(i);
                    Object columnValue = resultSet.getObject(i);
                    row.append(columnName, columnValue);
                }          
                output.add(row);
            }
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }                
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }   
        return output;
    }

    public Document executeUpdate(String query) throws Exception {
        Document output = new Document();
        Connection connection = null;
        // int output = 0;
        try {
            Class.forName(postgresqlClass);
            connection = DriverManager.getConnection(postgresqlUri, postgresqlUsername, postgresqlPassword);

            // Statement statement = connection.createStatement();
            PreparedStatement statement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            int executeUpdateResult = statement.executeUpdate();
            output.append("result", executeUpdateResult);
            ResultSet resultSet = statement.getGeneratedKeys();
            while (resultSet.next()) {
                // System.out.println("resultSet" + resultSet.getObject(1));
                output.append("generated_id", resultSet.getString(1));
            }
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }                
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return output;
    }
}