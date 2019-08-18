package com.indocms.postgresqlservice.controller;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.UUID;

import com.indocms.postgresqlservice.model.Response;
import com.indocms.postgresqlservice.service.DataFlowService;
import com.indocms.postgresqlservice.service.GeneralService;
import com.indocms.postgresqlservice.service.PostgresqlService;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@RestController
public class PostgresqlController {

    @Autowired
    private PostgresqlService postgresqlService;

    @Autowired
    private GeneralService generalService;

    @Autowired
    private DataFlowService dataFlowService;

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

    @RequestMapping(value = "/postgresql/export/{exportType}/{module}/{templateCode}", method = RequestMethod.POST)
    public Response exportData(@PathVariable String exportType, @PathVariable String module, @PathVariable String templateCode, @RequestBody (required = false) String requestBody) {
        Response response = null;
        Document meta = new Document();
        Document data = new Document();
        
        try {
            requestBody = requestBody == null ? "{}" : requestBody;
            data = dataFlowService.exportTaskExecution(exportType, module, templateCode, requestBody);
            meta = generalService.getHttpStatusCode("200");
        } catch (Exception e) {            
            e.printStackTrace();
            meta = generalService.getHttpStatusCode("400");
            meta.put("stacktrace", e.getMessage());
        }

        response = new Response(meta, data);

        return response;
    }

    @RequestMapping(value = "/postgresql/import/{module}/{templateCode}", method = RequestMethod.POST)
    public Response importData(
        @PathVariable String module, 
        @PathVariable String templateCode,
        @RequestParam("file") MultipartFile file) {

        Response response = null;
        Document meta = new Document();
        Document data = new Document();
        
        try {
            // String fileName = file.getOriginalFilename();
            // String fileDownloadUri = ServletUriComponentsBuilder.fromCurrentContextPath()
            //     .path("/downloadFile/")
            //     .path(fileName)
            //     .toUriString();
            // System.out.println("fileDownloadUri : " + fileDownloadUri);            
            // File destinationFile = new File("/home/emerio/Documents/nitip/indo-cms-import/" + UUID.randomUUID().toString() + ".csv");
            // Path destinationPath = Paths.get("/home/emerio/Documents/nitip/indo-cms-import/" + UUID.randomUUID().toString() + ".csv");            
            // Files.copy(file.getInputStream(), destinationPath, StandardCopyOption.REPLACE_EXISTING);
            data = dataFlowService.importTaskExecution(module, templateCode, file);
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