package com.indocms.postgresqlservice.controller;

import java.util.List;

import com.indocms.postgresqlservice.model.Response;
import com.indocms.postgresqlservice.service.GeneralService;
import com.indocms.postgresqlservice.service.PostgresqlService;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PostgresqlController {

    @Autowired
    private PostgresqlService postgresqlService;

    @Autowired
    private GeneralService generalService;

    @RequestMapping(value = "/postgresql/data/{module}/{templateCode}", method = RequestMethod.POST)
    public Response getData(@PathVariable String module, @PathVariable String templateCode) {
        Response response = null;
        Document meta = new Document();
        Document data = new Document();
        
        try {
            List<Document> dataDocument = postgresqlService.getData(module, templateCode);
            data.append("rdbms_data", dataDocument);
            meta = generalService.getHttpStatusCode("200");
        } catch (Exception e) {            
            e.printStackTrace();
            meta = generalService.getHttpStatusCode("400");
            meta.put("stacktrace", e.getMessage());
        }

        response = new Response(meta, data);

        return response;
    }

    @RequestMapping(value = "/postgresql/insert/{module}/{templateCode}", method = RequestMethod.POST)
    public Response insertData(@PathVariable String module, @PathVariable String templateCode, @RequestBody (required = false) String requestBody) {
        Response response = null;
        Document meta = new Document();
        Document data = new Document();
        
        try {
            requestBody = requestBody == null ? "{}" : requestBody;          
            data = postgresqlService.insertData(module, templateCode, requestBody);
            meta = generalService.getHttpStatusCode("200");
        } catch (Exception e) {            
            e.printStackTrace();
            meta = generalService.getHttpStatusCode("400");
            meta.put("stacktrace", e.getMessage());
        }

        response = new Response(meta, data);

        return response;
    }

    @RequestMapping(value = "/postgresql/update/{module}/{templateCode}", method = RequestMethod.POST)
    public Response updateData(@PathVariable String module, @PathVariable String templateCode, @RequestBody (required = false) String requestBody) {
        Response response = null;
        Document meta = new Document();
        Document data = new Document();
        
        try {
            requestBody = requestBody == null ? "{}" : requestBody;          
            data = postgresqlService.updateData(module, templateCode, requestBody);
            meta = generalService.getHttpStatusCode("200");
        } catch (Exception e) {            
            e.printStackTrace();
            meta = generalService.getHttpStatusCode("400");
            meta.put("stacktrace", e.getMessage());
        }

        response = new Response(meta, data);

        return response;
    }

    @RequestMapping(value = "/postgresql/delete/{module}/{templateCode}", method = RequestMethod.POST)
    public Response deleteData(@PathVariable String module, @PathVariable String templateCode, @RequestBody (required = false) String requestBody) {
        Response response = null;
        Document meta = new Document();
        Document data = new Document();
        
        try {
            requestBody = requestBody == null ? "{}" : requestBody;          
            data = postgresqlService.deleteData(module, templateCode, requestBody);
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