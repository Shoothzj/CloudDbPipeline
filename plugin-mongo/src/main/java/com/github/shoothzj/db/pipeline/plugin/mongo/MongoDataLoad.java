package com.github.shoothzj.db.pipeline.plugin.mongo;

import com.github.shoothzj.db.pipeline.api.exchange.MapExchange;
import com.github.shoothzj.db.pipeline.api.module.db.MongoInfoDto;
import com.github.shoothzj.db.pipeline.core.AbstractLoad;
import com.github.shoothzj.db.pipeline.core.mapper.AbstractGenericMapper;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.InsertOneResult;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public class MongoDataLoad extends AbstractLoad<MongoInfoDto> {

    private MongoInfoDto mongoInfoDto;

    private MongoClient mongoClient;

    private MongoDatabase database;

    private MongoCollection<Document> collection;

    private AbstractGenericMapper<Document> abstractGenericMapper;

    @Override
    public void init(MongoInfoDto mongoInfoDto) {
        this.mongoInfoDto = mongoInfoDto;
        final ConnectionString connectionString = new ConnectionString(mongoInfoDto.getConnectionStr());
        final MongoClientSettings.Builder builder = MongoClientSettings.builder().applyConnectionString(connectionString);
        builder.applyToConnectionPoolSettings(connectionPoolBuilder -> connectionPoolBuilder.maxSize(200));
        final MongoClientSettings mongoClientSettings = builder.build();
        mongoClient = MongoClients.create(mongoClientSettings);
        database = mongoClient.getDatabase(mongoInfoDto.getDbName());
        collection = database.getCollection(mongoInfoDto.getCollectionName());
        abstractGenericMapper = new MongoDocumentMapper();
    }

    @Override
    public boolean load(MapExchange mapExchange) {
        final InsertOneResult insertOneResult = collection.insertOne(abstractGenericMapper.map2Generic(mapExchange));
        return true;
    }

    @Override
    public boolean load(List<MapExchange> mapExchanges) {
        for (MapExchange mapExchange : mapExchanges) {
            load(mapExchange);
        }
        return true;
    }
}
