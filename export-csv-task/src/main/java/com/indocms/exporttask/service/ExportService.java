package com.indocms.exporttask.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

@Service
public class ExportService {

    @Value("${app.mongodb.database.main}")
    private String databaseName;

    @Value("${app.mongodb.collection.template.header}")
    private String templateHeaderCollection;

    @Value("${app.mongodb.collection.template.detail}")
    private String templateDetailCollection;

    @Value("${app.mongodb.collection.export.history}")
    private String exportHistoryCollection;

    @Value("${app.csv.row.separator}")
    private String rowSeparator;

    @Value("${app.csv.column.delimiter}")
    private String columnDelimiter;

    @Autowired
    private MongoService mongoService;

    @Autowired
    private PostgresqlService postgresqlService;

    String exportId = null;
    String module = null;
    String templateCode = null;
    Document exportHistoryDoc = null;
    Document templateHeader = null;
    List<Document> templateDetail = null;
    StringBuilder exportQuery = null;

    public void process(ApplicationArguments args) throws Exception {
        exportId = args.getOptionValues("export_id").get(0);
        Document exportHistoryFilter = new Document("_id", new ObjectId(exportId));
        List<Document> exportHistoryDocList = mongoService.find(databaseName, exportHistoryCollection, exportHistoryFilter, null, null);
        if (exportHistoryDocList.size() > 0) {
            exportHistoryDoc = (Document) exportHistoryDocList.get(0);
            module = exportHistoryDoc.get("module").toString();
            templateCode = exportHistoryDoc.get("template_code").toString();
            this.getTemplateHeader();
            this.getTemplateDetail();
            this.generateExportQuery();

            this.exportCsv();
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

    public void generateExportQuery() {
        String databaseName = templateHeader.getString("database");
        String databaseTableSeparator = templateHeader.getString("database_table_separator") != null ? templateHeader.getString("database_table_separator") : "";
        String tableName = templateHeader.getString("table");

        exportQuery = new StringBuilder("SELECT ");
        int count = 0;
        for (Document row : templateDetail) {
            String columnQuery = row.getString("column_query");
            exportQuery.append(columnQuery);

            if (count + 1 < templateDetail.size()) {
                exportQuery.append(", ");
            }
            count++;
        }
        exportQuery.append(" FROM ").append(databaseName).append(databaseTableSeparator).append(tableName);
        System.out.println("query export = " + exportQuery);
    }

    public void exportCsv() throws Exception {        
        System.out.println("(CSV)");        
        BufferedWriter bufferedWriter = null;
        try {
            File exportFile = new File(exportHistoryDoc.getString("export_path"));
            System.out.println("exportFile : " + exportFile.getAbsolutePath());
            bufferedWriter = new BufferedWriter(new FileWriter(exportFile));
            int count = 0;
            for (Document row : templateDetail) {
                String columnTitle = row.getString("column_title");
                bufferedWriter.write(columnTitle);
    
                if (count + 1 < templateDetail.size()) {
                    bufferedWriter.write(columnDelimiter);
                }
                count++;
            }
            bufferedWriter.write(rowSeparator);
            
            List<Document> queryResult = postgresqlService.executeQuery(exportQuery.toString());
            for (Document row : queryResult) {
                count = 0;
                for (Document column : templateDetail) {
                    String columnDatabase = column.getString("column_id");
                    String rowValue = row.get(columnDatabase).toString();
                    bufferedWriter.write(rowValue);

                    if (count + 1 < templateDetail.size()) {
                        bufferedWriter.write(columnDelimiter);
                    }
                    count++;
                }
                bufferedWriter.write(rowSeparator);
            }
        } finally {
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }            
        }
    }
}