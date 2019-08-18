package com.indocms.importtask.service;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class MongoService {

    @Value("${app.mongodb.uri}")
    private String mongoDBUri;

    private MongoClient mongoClient = null;

    public List<Document> find(String databaseName, String collectionName, Document filter, Document sort, Document projection) throws Exception {
        List<Document> output = new ArrayList<>();
        try {
            // System.out.println("mongoDBUri : " + mongoDBUri);
            MongoClientURI mongoClientURI = new MongoClientURI(mongoDBUri);        
            mongoClient = new MongoClient(mongoClientURI);
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collectionName);
    
            filter = filter == null ? new Document() : filter;
            sort = sort == null ? new Document() : sort;
            projection = projection == null ? new Document() : projection;
    
            output = mongoCollection.find(filter).sort(sort).projection(projection).into(new ArrayList<>());
        } finally {
            mongoClient.close();
        }        
        return output;
    }
}