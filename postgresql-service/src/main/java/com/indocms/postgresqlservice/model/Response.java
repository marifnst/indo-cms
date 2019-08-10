package com.indocms.postgresqlservice.model;

import org.bson.Document;

public class Response {
    Document meta, data;

    public Response() {
    }    

    public Response(Document meta, Document data) {
        this.meta = meta;
        this.data = data;
    }    

    public Document getMetadata() {
        return meta;
    }

    public void setMeta(Document meta) {
        this.meta = meta;
    }

    public Document getData() {
        return data;
    }

    public void setData(Document data) {
        this.data = data;
    }
}