package com.indocms.importtask.service;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
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
    List<String> xlsColumns = null;
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

            this.importExcel();
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

    public void importExcel() throws Exception {        
        System.out.println("(Excel)");
        String databaseName = templateHeader.getString("database");
        String databaseTableSeparator = templateHeader.getString("database_table_separator") != null ? templateHeader.getString("database_table_separator") : "";
        String tableName = templateHeader.getString("table");
        
        String importFilePath = importHistoryDoc.getString("import_path");
        try (InputStream inp = new FileInputStream(importFilePath)) {
            Workbook wb = WorkbookFactory.create(inp);
            Sheet sheet = wb.getSheetAt(0);
            xlsColumns = new ArrayList<>();

            for (int i = 0;i <= sheet.getLastRowNum();i++) {
                Row row = sheet.getRow(i);
                updateQuery = new StringBuilder("UPDATE ").append(databaseName).append(databaseTableSeparator).append(tableName).append(" SET ");
                insertQuerySufix = new StringBuilder();
                primaryKeyDoc = new Document();

                for (int j = 0;j < row.getLastCellNum();j++) {
                    Cell cell = row.getCell(j);
                    String cellValue = this.getExcellCellValue(cell);
                    // System.out.println("cellvalue : " + cellValue);

                    if (i == 0) {
                        xlsColumns.add(cellValue);
                    } else {
                        String databaseColumn = templateDetail.get(j).getString("column_query");
                        if (templateDetail.get(j).containsKey("is_primary") && templateDetail.get(j).getBoolean("is_primary")) {
                            primaryKeyDoc.append("column_index", j);
                            primaryKeyDoc.append("column_name", templateDetail.get(j).get("column_query"));
                            if (cellValue != null && !cellValue.equals("")) {
                                primaryKeyDoc.append("column_value", cellValue);
                            }
                        } else {                            
                            insertQuerySufix.append("'").append(cellValue).append("'");
                            updateQuery.append(databaseColumn).append(" = '").append(cellValue).append("'");
                            if (j + 1 < row.getLastCellNum()) {
                                insertQuerySufix.append(", ");
                                updateQuery.append(", ");
                            }  
                        }                                              
                    }
                }

                if (i > 0) {
                    if (primaryKeyDoc.containsKey("column_value")) {
                        String primaryKeyColumn = primaryKeyDoc.getString("column_name");
                        String primaryKeyValue = primaryKeyDoc.getString("column_value");
                        updateQuery.append(" WHERE ").append(primaryKeyColumn).append(" = '").append(primaryKeyValue).append("'");
                        System.out.println("updateQuery : " + updateQuery);
                        try {
                            postgresqlService.executeUpdate(updateQuery.toString());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }                        
                    } else {
                        insertQueryPrefix = new StringBuilder("INSERT INTO ").append(databaseName).append(databaseTableSeparator).append(tableName).append(" (");
                        for (int j = 0;j < xlsColumns.size();j++) {
                            if (j != primaryKeyDoc.getInteger("column_index")) {
                                String databaseColumn = templateDetail.get(j).getString("column_query");
                                insertQueryPrefix.append(databaseColumn);
    
                                if (j + 1 < xlsColumns.size()) {
                                    insertQueryPrefix.append(", ");
                                }
                            }
                        }
                        insertQuerySufix.append(")");
                        insertQueryPrefix.append(") VALUES (").append(insertQuerySufix.toString());
                        System.out.println("insertQueryPrefix : " + insertQueryPrefix);
                        
                        try {
                            postgresqlService.executeUpdate(insertQueryPrefix.toString());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }                        
                    }
                }
            }
        }  finally {
            
        }
    }

    public String getExcellCellValue(Cell cell) {
        String output = null;
        switch (cell.getCellType()) {
            case NUMERIC : {
                output = String.valueOf(cell.getNumericCellValue());
                break;
            }
            default: {
                output = cell.getStringCellValue();
                break;
            }
        }
        return output;
    }
}