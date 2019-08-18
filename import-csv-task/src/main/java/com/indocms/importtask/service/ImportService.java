package com.indocms.importtask.service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

@Service
public class ImportService {

    @Value("${app.mongodb.database.main}")
    private String databaseName;

    @Value("${app.mongodb.collection.template.header}")
    private String templateHeaderCollection;

    @Value("${app.mongodb.collection.template.detail}")
    private String templateDetailCollection;

    @Value("${app.mongodb.collection.import.history}")
    private String importHistoryCollection;

    @Value("${app.csv.column.delimiter}")
    private String columnDelimiter;

    @Autowired
    private MongoService mongoService;

    @Autowired
    private PostgresqlService postgresqlService;

    String importId = null;
    String module = null;
    String templateCode = null;
    Document importHistoryDoc = null;
    Document templateHeader = null;
    List<Document> templateDetail = null;
    StringBuilder exportQuery = null;
    List<String> csvColumns = null;
    Document primaryKeyDoc = null;
    StringBuilder insertQueryPrefix = null;
    StringBuilder insertQuerySufix = null;
    StringBuilder updateQuery = null;

    public void process(ApplicationArguments args) throws Exception {
        importId = args.getOptionValues("import_id").get(0);
        Document importHistoryFilter = new Document("_id", new ObjectId(importId));
        List<Document> importHistoryDocList = mongoService.find(databaseName, importHistoryCollection, importHistoryFilter, null, null);
        if (importHistoryDocList.size() > 0) {
            importHistoryDoc = (Document) importHistoryDocList.get(0);
            module = importHistoryDoc.get("module").toString();
            templateCode = importHistoryDoc.get("template_code").toString();
            this.getTemplateHeader();
            this.getTemplateDetail();

            this.importCsv();
        }
    }

    public void getTemplateHeader() throws Exception {
        Document filter = new Document();
        filter.append("module", module);
        filter.append("template_code", templateCode);
        templateHeader = mongoService.find(databaseName, templateHeaderCollection, filter, null, null).get(0);
    }

    public void getTemplateDetail() throws Exception {
        Document filter = new Document();
        filter.append("module", module);
        filter.append("template_code", templateCode);

        Document sort = new Document("sequence", 1);
        templateDetail = mongoService.find(databaseName, templateDetailCollection, filter, sort, null);
    }

    public void importCsv() throws Exception {        
        System.out.println("(CSV)");
        BufferedReader bufferedReader = null;
        String databaseName = templateHeader.getString("database");
        String databaseTableSeparator = templateHeader.getString("database_table_separator") != null ? templateHeader.getString("database_table_separator") : "";
        String tableName = templateHeader.getString("table");
        
        try {
            String csvPathFile = importHistoryDoc.getString("import_path");
            bufferedReader = new BufferedReader(new FileReader(csvPathFile));
            String rowString = bufferedReader.readLine();
            int rowCount = 0;
            while (rowString != null) {
                if (rowCount == 0) {
                    csvColumns = Arrays.asList(rowString.split("\\" + columnDelimiter));                    
                } else {
                    List<String> columns = Arrays.asList(rowString.split("\\" + columnDelimiter));                    
                    int columnCount = 0;                    
                    insertQuerySufix = new StringBuilder(" VALUES (");
                    updateQuery = new StringBuilder("UPDATE ").append(databaseName).append(databaseTableSeparator).append(tableName).append(" SET ");
                    for (String column : columns) {
                        // System.out.println(">>>>>>>>>>>>>>>> " + column);
                        if (templateDetail.get(columnCount).containsKey("is_primary") && templateDetail.get(columnCount).getBoolean("is_primary")) {
                            primaryKeyDoc.append("column_index", columnCount);
                            primaryKeyDoc.append("column_name", templateDetail.get(columnCount).get("column_query"));
                            if (column != null && !column.equals("")) {
                                primaryKeyDoc.append("column_value", column);
                            }
                        } else {
                            insertQuerySufix.append("'").append(column).append("'");
                            String columnName = templateDetail.get(columnCount).getString("column_query");
                            updateQuery.append(columnName).append(" = '").append(column).append("'");

                            if (columnCount + 1 < columns.size()) {
                                insertQuerySufix.append(",");
                                updateQuery.append(", ");
                            }
                        }
                        columnCount++;
                    }

                    if (primaryKeyDoc.containsKey("column_value")) {
                        String primaryKeyColumn = primaryKeyDoc.getString("column_name");
                        String primaryKeyValue = primaryKeyDoc.getString("column_value");
                        updateQuery.append(" WHERE ").append(primaryKeyColumn).append(" = '").append(primaryKeyValue).append("'");

                        System.out.println("updateQuery : " + updateQuery);
                        postgresqlService.executeUpdate(updateQuery.toString());                        
                    } else {
                        insertQueryPrefix = new StringBuilder("INSERT INTO ").append(databaseName).append(databaseTableSeparator).append(tableName).append(" (");
                        for (int i = 0;i < csvColumns.size();i++) {
                            if (i != primaryKeyDoc.getInteger("column_index")) {
                                insertQueryPrefix.append(templateDetail.get(i).getString("column_query"));
                                if (i + 1 < csvColumns.size()) {
                                    insertQueryPrefix.append(", ");
                                }
                            }
                        }
                        insertQueryPrefix.append(") ");
                        insertQuerySufix.append(")");

                        // append suffix to prefix
                        insertQueryPrefix.append(insertQuerySufix.toString());

                        System.out.println("insertQueryPrefix : " + insertQueryPrefix);
                        postgresqlService.executeUpdate(insertQueryPrefix.toString());                        
                    }
                }               
                
                System.out.println("########################################################");
                rowCount++;
                primaryKeyDoc = new Document();
                rowString = bufferedReader.readLine();
            }
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }        
    }
}