package com.github.shoothzj.db.pipeline.plugin.mongo;

import com.github.shoothzj.db.pipeline.core.AbstractRewind;
import com.github.shoothzj.db.pipeline.api.module.SourceBrief;
import com.github.shoothzj.db.pipeline.api.module.db.MongoInfoDto;
import com.github.shoothzj.db.pipeline.api.module.transform.TransformDto;
import com.github.shoothzj.db.pipeline.plugin.mongo.util.MongoCalculateUtil;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.ArrayList;

/**
 * @author hezhangjian
 */
@Slf4j
public class MongoDataRewind<PT> extends AbstractRewind<MongoInfoDto, PT> {

    private MongoInfoDto mongoInfoDto;

    private TransformDto transformDto;

    private MongoClient mongoClient;

    private MongoDatabase database;

    private MongoCollection<Document> collection;

    public MongoDataRewind() {

    }

    @Override
    public void init(MongoInfoDto mongoInfoDto, TransformDto transformDto) {
        this.mongoInfoDto = mongoInfoDto;
        this.transformDto = transformDto;
        final ConnectionString connectionString = new ConnectionString(mongoInfoDto.getConnectionStr());
        final MongoClientSettings.Builder builder = MongoClientSettings.builder().applyConnectionString(connectionString);
        builder.applyToConnectionPoolSettings(connectionPoolBuilder -> connectionPoolBuilder.maxSize(200));
        final MongoClientSettings mongoClientSettings = builder.build();
        mongoClient = MongoClients.create(mongoClientSettings);
        database = mongoClient.getDatabase(mongoInfoDto.getDbName());
        collection = database.getCollection(mongoInfoDto.getCollectionName());
    }

    @Override
    public SourceBrief<PT> detectBrief() {
        return null;
    }

    @Override
    public boolean rewind() {
        boolean first = true;
        Object cursor = null;
        long count = 0;

        FindIterable<Document> documents;
        while (true) {
            if (first) {
                documents = collection.find().sort(new BasicDBObject("_id", 1)).limit(10);
                first = false;
            } else {
                documents = collection.find(Filters.gt("_id", cursor)).sort(new BasicDBObject("_id", 1)).limit(10);
            }
            final ArrayList<Document> arrayList = Lists.newArrayList(documents);
            if (arrayList.size() == 0) {
                break;
            }
            count += arrayList.size();
            for (Document document : arrayList) {
                processSingleItem(document);
            }
            final Document document = arrayList.get(arrayList.size() - 1);
            cursor = document.get("_id");
            log.info("cursor is [{}]", cursor);
        }
        return count >= 0;
    }

    @Override
    public boolean rewind(PT start, PT end) {
        return false;
    }

    private void processSingleItem(Document document) {
        final Document mapDocument = MongoCalculateUtil.mapDocument(document, transformDto.getMap());
        collection.replaceOne(Filters.eq("_id", mapDocument.get("_id")), mapDocument);
    }

    @Override
    public void close() {
        mongoClient.close();
    }
}
