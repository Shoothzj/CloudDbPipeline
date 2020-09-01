package com.github.shoothzj.db.pipeline;

import com.github.shoothzj.javatool.util.EnvUtil;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ConnectionPoolSettings;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;

import static com.mongodb.client.model.Filters.gt;

/**
 * @author hezhangjian
 */
@Slf4j
public class TraverseTable {

    public static void main(String[] args) {
        final String connectionStr = EnvUtil.getStringVar("mongo.connection.string", "MONGO_CONNECTION_STR", "mongodb://localhost:27017");
        final String mongoDatabase = EnvUtil.getStringVar("mongo.database", "MONGO_DATABASE", "ttbb");
        final String mongoCollection = EnvUtil.getStringVar("mongo.collection", "MONGO_COLLECTION", "user");

        final MongoClientSettings.Builder builder = MongoClientSettings.builder().applyConnectionString(new ConnectionString(connectionStr));
        builder.applyToConnectionPoolSettings(connectionPoolBuilder -> connectionPoolBuilder.maxSize(200));
        final MongoClientSettings mongoClientSettings = builder.build();

        final MongoClient mongoClient = MongoClients.create(mongoClientSettings);
        final MongoDatabase database = mongoClient.getDatabase(mongoDatabase);
        final MongoCollection<Document> collection = database.getCollection(mongoCollection);

        boolean first = true;
        Object cursor = null;
        long count = 0;

        final long startTime = System.currentTimeMillis();
        FindIterable<Document> documents;
        while (true) {
            if (first) {
                documents = collection.find().sort(new BasicDBObject("_id", 1)).limit(10);
                first = false;
            } else {
                documents = collection.find(gt("_id", cursor)).sort(new BasicDBObject("_id", 1)).limit(10);
            }
            final ArrayList<Document> arrayList = Lists.newArrayList(documents);
            count += arrayList.size();
            if (arrayList.size() == 0) {
                break;
            }
            final Document document = arrayList.get(arrayList.size() - 1);
            cursor = document.get("_id");
            log.info("cursor is [{}]", cursor);
        }
        log.info("read count is [{}], cost is [{}]", count, System.currentTimeMillis() - startTime);
    }

}
