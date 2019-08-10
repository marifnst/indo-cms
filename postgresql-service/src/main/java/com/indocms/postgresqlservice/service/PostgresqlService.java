package com.indocms.postgresqlservice.service;

import java.sql.Connection;
import java.sql.DriverManager;
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

        Connection connection = null;
        try {
            templateHeader = this.getTemplateHeader(module, templateCode);

            Class.forName(postgresqlClass);
            connection = DriverManager.getConnection(postgresqlUri, postgresqlUsername, postgresqlPassword);

            Statement statement = connection.createStatement();
            // String query = "select * from \"INDO_CMS_SAMPLE_REPORT\"";
            String query = templateHeader.get("query_select").toString();
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
}