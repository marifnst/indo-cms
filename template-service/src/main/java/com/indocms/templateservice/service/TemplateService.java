package com.indocms.templateservice.service;

import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class TemplateService {

    @Value("${app.mongodb.database.main}")
    private String mainDatabase;

    @Value("${app.mongodb.collection.template.header}")
    private String templateHeaderCollection;
    
    @Autowired
    private MongoService mongoService;

    private Document templateHeader = new Document();

    public Document getTemplateData(String module, String templateCode) throws Exception {
        templateHeader = this.getTemplateHeader(module, templateCode);        
        return templateHeader;
    }

    public Document getTemplateHeader(String module, String templateCode) throws Exception {
        Document output = null;
        Document filter = new Document("module", module).append("template_code", templateCode);
        Document projection = new Document("_id", 0);
        List<Document> tmpOutput = mongoService.find(mainDatabase, templateHeaderCollection, filter, null, projection);
        if (tmpOutput.size() > 0) {
            output = tmpOutput.get(0);
        } else {
            throw new Exception("Template Code (" + templateCode + ") Not Found");
        }
        return output;
    }
}