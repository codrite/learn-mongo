package com.codrite.mongolearning;


import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class MongoTest {

    final int port = 30891;

    @Test(timeout = 1000L)
    public void assert_that_mongo_connection_is_alive() {
        MongoClient mongoClient = new MongoClient("localhost", port);
        Assert.assertNotNull(mongoClient.listDatabaseNames().first());
    }

    @Test(timeout = 1000)
    public void assert_that_database_with_name_chiya_exists() {
        MongoClient mongoClient = new MongoClient("localhost", port);
        Assert.assertNotNull(mongoClient.listDatabaseNames().first());
        MongoDatabase mongoDatabase = mongoClient.getDatabase("chiya");
        Assert.assertEquals("chiya", mongoDatabase.getName());
    }

    @Test(timeout = 1000)
    public void assert_that_collection_name_template_is_found_in_database_chiya() {
        MongoClient mongoClient = new MongoClient("localhost", port);
        Assert.assertNotNull(mongoClient.listDatabaseNames().first());
        MongoDatabase mongoDatabase = mongoClient.getDatabase("chiya");
        Assert.assertEquals("chiya", mongoDatabase.getName());

        if (mongoDatabase.getCollection("Template").countDocuments() == 0)
            mongoDatabase.createCollection("Template");

        MongoCollection<Document> templateCollection = mongoDatabase.getCollection("Template");
        Assert.assertEquals("Template", templateCollection.getNamespace().getCollectionName());
    }

    @Test(timeout = 1000)
    public void assert_that_a_single_document_was_inserted_in_collection_name_template() {
        MongoClient mongoClient = new MongoClient("localhost", port);
        Assert.assertNotNull(mongoClient.listDatabaseNames().first());
        MongoDatabase mongoDatabase = mongoClient.getDatabase("chiya");
        Assert.assertEquals("chiya", mongoDatabase.getName());

        if (mongoDatabase.getCollection("Template").countDocuments() == 0)
            mongoDatabase.createCollection("Template");

        MongoCollection<Document> templateCollection = mongoDatabase.getCollection("Template");
        Map<String, Object> map = new HashMap<>();
        String key = "name" + System.currentTimeMillis();
        map.put(key, "person");
        Document tDocument = new Document(map);
        templateCollection.insertOne(tDocument);
        FindIterable<Document> search = templateCollection.find(tDocument);
        Assert.assertEquals("person", search.first().get(key));
    }


}
