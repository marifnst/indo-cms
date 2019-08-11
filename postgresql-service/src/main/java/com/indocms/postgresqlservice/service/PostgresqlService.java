package com.indocms.postgresqlservice.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class PostgresqlService {

    @Value("${app.mongodb.database.main}")
    private String mainDatabase;

    @Value("${app.mongodb.collection.template.header}")
    private String templateHeaderCollection;
    
    @Value("${app.postgresql.class}")
    private String postgresqlClass;

    @Value("${app.postgresql.uri}")
    private String postgresqlUri;

    @Value("${app.postgresql.username}")
    private String postgresqlUsername;

    @Value("${app.postgresql.password}")
    private String postgresqlPassword;

    @Autowired
    private MongoService mongoService;

    private Document templateHeader = new Document();

    public List<Document> getData(String module, String templateCode) throws Exception {
        List<Document> output= new ArrayList<>();
        templateHeader = this.getTemplateHeader(module, templateCode);
        String query = templateHeader.get("query_select").toString();
        output = this.executeQuery(query);
        return output;
     }

     public Document insertData(String module, String templateCode, String requestBody) throws Exception {
        Document output = new Document();
        if (requestBody == null || requestBody.equals("{}")) {
            throw new Exception("Request body cannot be null or blank");
        } else {
            templateHeader = this.getTemplateHeader(module, templateCode);
            Document requestBodyDoc = Document.parse(requestBody);   
            System.out.println("this.convertReqBodyToInsertQuery(requestBodyDoc) : " + this.convertReqBodyToInsertQuery(requestBodyDoc));
            String query = this.convertReqBodyToInsertQuery(requestBodyDoc);
            Document executeUpdateDoc = this.executeUpdate(query);
            int executeUpdateResult = executeUpdateDoc.getInteger("result");            
            if (executeUpdateResult == 0) {
                output.append("message", "Insert Failed");
            } else {
                output.append("message", "Insert Success");
                if (executeUpdateDoc.get("generated_id") != null) {
                    String executeUpdateGenId = executeUpdateDoc.get("generated_id").toString();
                    String insertId = templateHeader.get("insert_id").toString();
                    requestBodyDoc.append(insertId, executeUpdateGenId);
                }                                 
                output.append("insert_data", requestBodyDoc);
            }
            output.append("query", query);
        }        
        return output;
     }

     public Document updateData(String module, String templateCode, String requestBody) throws Exception {
        Document output = new Document();
        if (requestBody == null || requestBody.equals("{}")) {
            throw new Exception("Request body cannot be null or blank");
        } else {
            templateHeader = this.getTemplateHeader(module, templateCode);
            Document requestBodyDoc = Document.parse(requestBody);   
            System.out.println("this.convertReqBodyToUpdateQuery(requestBodyDoc) : " + this.convertReqBodyToUpdateQuery(requestBodyDoc));
            String query = this.convertReqBodyToUpdateQuery(requestBodyDoc);
            Document executeUpdateDoc = this.executeUpdate(query);
            System.out.println("executeUpdateDoc : " + executeUpdateDoc.toJson());
            output.append("query", query);
        }        
        return output;
     }

     public Document deleteData(String module, String templateCode, String requestBody) throws Exception {
        Document output = new Document();
        if (requestBody == null || requestBody.equals("{}")) {
            throw new Exception("Request body cannot be null or blank");
        } else {
            templateHeader = this.getTemplateHeader(module, templateCode);
            Document requestBodyDoc = Document.parse(requestBody);   
            System.out.println("this.convertReqBodyToDeleteQuery(requestBodyDoc) : " + this.convertReqBodyToDeleteQuery(requestBodyDoc));
            String query = this.convertReqBodyToDeleteQuery(requestBodyDoc);
            Document executeUpdateDoc = this.executeUpdate(query);
            System.out.println("executeUpdateDoc : " + executeUpdateDoc.toJson());
            output.append("query", query);
        }        
        return output;
     }

     public Document getTemplateHeader(String module, String templateCode) throws Exception {
        Document output = null;
        Document filter = new Document("module", module).append("template_code", templateCode);
        Document projection = new Document();
        List<Document> tmpOutput = mongoService.find(mainDatabase, templateHeaderCollection, filter, null, projection);
        if (tmpOutput.size() > 0) {
            output = tmpOutput.get(0);
        } else {
            throw new Exception("Template Code (" + templateCode + ") Not Found");
        }
        return output;
    }

    public String convertReqBodyToInsertQuery(Document requestBodyDoc) {
        StringBuilder output = new StringBuilder("INSERT INTO ");
        StringBuilder values = new StringBuilder("(");

        String databaseName = templateHeader.get("database").toString();
        String databaseTableSeparator = templateHeader.get("database_table_separator").toString();
        String tableName = templateHeader.get("table").toString();
        
        output.append(databaseName).append(databaseTableSeparator).append(tableName);
        output.append(" (");

        int count = 0;
        for (String key : requestBodyDoc.keySet()) {
            Object keyValue = requestBodyDoc.get(key);
            output.append("\"").append(key).append("\"");
            values.append("'").append(keyValue).append("'");

            if (count + 1 < requestBodyDoc.keySet().size()) {
                output.append(", ");
                values.append(", ");
            }
            count++;
        }
        values.append(")");        
        output.append(") VALUES ").append(values);
        return output.toString();
    }

    public String convertReqBodyToUpdateQuery(Document requestBodyDoc) {
        StringBuilder output = new StringBuilder("UPDATE ");
        String filterQuery = "";

        String databaseName = templateHeader.get("database").toString();
        String databaseTableSeparator = templateHeader.get("database_table_separator").toString();
        String tableName = templateHeader.get("table").toString();
        
        output.append(databaseName).append(databaseTableSeparator).append(tableName);
        output.append(" SET ");

        String updateId = templateHeader.get("update_id").toString();
        for (String key : requestBodyDoc.keySet()) {
            Object keyValue = requestBodyDoc.get(key);
            if (key.equals(updateId)) {
                filterQuery = " WHERE \"" + updateId + "\" = '" + keyValue + "'";
            } else {                
                output.append("\"" + key + "\"").append(" = '").append(keyValue).append("', ");
            }
        }

        String tmpQuery = output.toString();
        output = new StringBuilder(tmpQuery.substring(0, tmpQuery.length() - 2));
        output.append(filterQuery);
        return output.toString();
    }

    public String convertReqBodyToDeleteQuery(Document requestBodyDoc) {
        StringBuilder output = new StringBuilder("DELETE FROM ");

        String databaseName = templateHeader.get("database").toString();
        String databaseTableSeparator = templateHeader.get("database_table_separator").toString();
        String tableName = templateHeader.get("table").toString();
        
        output.append(databaseName).append(databaseTableSeparator).append(tableName);

        String deleteId = templateHeader.get("delete_id").toString();
        String deleteIdValue = requestBodyDoc.get(deleteId).toString();
        output.append(" WHERE \"").append(deleteId).append("\" = '").append(deleteIdValue).append("'");

        return output.toString();
    }

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