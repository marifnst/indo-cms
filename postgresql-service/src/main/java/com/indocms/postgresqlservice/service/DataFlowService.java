package com.indocms.postgresqlservice.service;

import java.net.URI;
import java.util.ArrayList;
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

    @Value("${app.spring.cloud.dataflow.url}")
    private String springCloudDataFlowUrl;

    @Autowired
    private Environment environment;

    Map<String, String> properties = null;
    List<String> arguments = null;

    public Document exportTaskExecution(String exportType, String module, String templateCode, String payload) throws Exception {
        Document output = new Document();
        try {            
            URI springCloudDataFlowUri = new URI(springCloudDataFlowUrl);
            String exportTaskName = environment.getProperty("app.export.task.name." + exportType.toLowerCase());

            propertyExtraction();
            argumentExtraction("--export_id=5d525575ca5bf47440f1b035");

            DataFlowTemplate dataFlowTemplate = new DataFlowTemplate(springCloudDataFlowUri);
            long taskExecutionId = dataFlowTemplate.taskOperations().launch(exportTaskName, properties, arguments, null);
            output.put("execution_id", taskExecutionId);
        } finally {

        }
        return output;
    }

    public void propertyExtraction() {
        properties = new HashMap<>();
    }

    public void argumentExtraction(String exportIdArgument) {
        arguments = new ArrayList<>();
        arguments.add(exportIdArgument);
    }
}