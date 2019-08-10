package com.indocms.templateservice.controller;

import com.indocms.templateservice.model.Response;
import com.indocms.templateservice.service.GeneralService;
import com.indocms.templateservice.service.TemplateService;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TemplateController {
    
    @Autowired
    private TemplateService templateService;

    @Autowired
    private GeneralService generalService;

    @RequestMapping(value ="/template/init/{module}/{templateCode}", method = RequestMethod.POST)
    public Response getTemplate(@PathVariable String module, @PathVariable String templateCode) {
        Response response = null;
        Document meta = new Document();
        Document data = new Document();

        try {
            data = templateService.getTemplateData(module, templateCode);
            meta = generalService.getHttpStatusCode("200");
        } catch (Exception e) {
            e.printStackTrace();
            meta = generalService.getHttpStatusCode("400");
            meta.put("stacktrace", e.getMessage());
        }
        response = new Response(meta, data);
        return response;
    }
}
