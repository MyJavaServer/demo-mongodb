package com.jayyin.demo.demomongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoMongodbApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoMongodbApplication.class, args);

        MongoDBJDBC jdbc = MongoDBJDBC.getInstance();

        MongoDatabase database = jdbc.connect("localhost", "27017", "", "", "test_db");

        MongoCollection collection = jdbc.getCollection(database, "test_db");


        jdbc.insert(collection);

        jdbc.getDocument(collection, null);

        jdbc.close();

    }

}
