package com.indocms.postgresqlservice.service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class GeneralService {

    @Value("${app.mongodb.database.main}")
    private String mainDatabase;

    @Value("${app.mongodb.collection.http.status}")
    private String httpStatusCollection;

    @Autowired
    private MongoService mongoService;

    private SimpleDateFormat simpleDateFormat = null;

    public Document getHttpStatusCode(String code) {
        Document output = new Document();
        try {
            Document filter = new Document("code", code);
            List<Document> tmpOutput = mongoService.find(mainDatabase, httpStatusCollection, filter, null, null);
            output.put("code", code);
            output.put("message", tmpOutput.get(0).get("message"));
        } catch (Exception e) {
            e.printStackTrace();
            output.put("code", "400");
            output.put("message", "Bad Request, HTTP Status Code Not Found");
        }
        return output;
    }

    public String convertDateToString(String datePattern, Date date) {
        String output = null;
        simpleDateFormat = new SimpleDateFormat(datePattern);
        output = simpleDateFormat.format(date);
        return output;
    }
}