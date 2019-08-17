package com.indocms.postgresqlservice.service;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.dataflow.rest.client.DataFlowTemplate;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Service
public class DataFlowService {

    @Value("${app.mongodb.database.main}")
    private String mainDatabase;

    @Value("${app.mongodb.collection.parameter}")
    private String parameterCollection;

    @Value("${app.export.parameter.code}")
    private String exportParameterCode;

    @Value("${app.mongodb.collection.export.history}")
    private String exportHistoryCollection;

    @Value("${app.export.date.pattern}")
    private String exportDatePattern;

    @Value("${app.spring.cloud.dataflow.url}")
    private String springCloudDataFlowUrl;

    @Autowired
    private Environment environment;

    @Autowired
    private MongoService mongoService;

    @Autowired
    private GeneralService generalService;

    Map<String, String> properties = new HashMap<>();
    List<String> arguments = null;

    public Document exportTaskExecution(String exportType, String module, String templateCode, String payload) throws Exception {
        Document output = new Document();
        try {            
            URI springCloudDataFlowUri = new URI(springCloudDataFlowUrl);
            String exportTaskName = environment.getProperty("app.export.task.name." + exportType.toLowerCase());

            String exportPath = getExportPath(exportType, module, templateCode);
            Document exportHistoryOutput = injectToExportHistory(exportType, module, templateCode, payload, exportPath);
            // argumentExtraction("--export_id=5d525575ca5bf47440f1b035");
            argumentExtraction(exportHistoryOutput);

            DataFlowTemplate dataFlowTemplate = new DataFlowTemplate(springCloudDataFlowUri);
            long taskExecutionId = dataFlowTemplate.taskOperations().launch(exportTaskName, properties, arguments, null);
            output.put("execution_id", taskExecutionId);
        } finally {

        }
        return output;
    }

    public String getExportPath(String exportType, String module, String templateCode) throws Exception {        
        String output = null;
        Document filter = new Document();
        filter.append("module", module);
        filter.append("parameter_code", exportParameterCode);
        List<Document> parameterOutput = mongoService.find(mainDatabase, parameterCollection, filter, null, null);
        if (parameterOutput.size() > 0) {
            String dateFile = generalService.convertDateToString(exportDatePattern, new Date());
            if (parameterOutput.get(0).getString("parameter_value") == null || parameterOutput.get(0).getString("parameter_value").equals("")) {
                throw new Exception("Export Parameter " + exportParameterCode + " Cannot Be Null or Blank");
            } else {
                output = parameterOutput.get(0).getString("parameter_value") + "/" + module + "_" + templateCode + "_" + dateFile + "." + exportType;
            }            
        } else {
            throw new Exception("Export Parameter Code Not Found");
        }

        return output;
    }

    public Document injectToExportHistory(String exportType, String module, String templateCode, String payload, String exportPath) throws Exception {
        Document output = new Document();
        output.append("export_type", exportType);
        output.append("module", module);
        output.append("template_code", templateCode);
        output.append("filter", Document.parse(payload));
        output.append("export_path", exportPath);
        
        mongoService.insertOneDocument(mainDatabase, exportHistoryCollection, output);
        return output;
    }

    public void argumentExtraction(Document exportArgument) {
        arguments = new ArrayList<>();
        String exportIdArgument = "--export_id=" + exportArgument.getObjectId("_id").toHexString();
        arguments.add(exportIdArgument);
    }
}